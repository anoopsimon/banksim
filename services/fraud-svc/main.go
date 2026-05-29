package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

var (
	fLog           *slog.Logger
	checksTotal    metric.Int64Counter
	flaggedTotal   metric.Int64Counter
	scoreHist      metric.Float64Histogram
	ruleLatency    metric.Float64Histogram
)

var badActors = map[string]bool{
	"ACC0013": true, "ACC0042": true, "ACC0666": true,
	"ACC0007": true, "ACC1313": true,
}

func main() {
	ctx := context.Background()
	shutdown := initOTEL(ctx, "fraud-svc")
	defer shutdown(ctx)

	fLog = otelslog.NewLogger("fraud-svc")
	m := otel.Meter("banksim.fraud-svc")

	checksTotal, _  = m.Int64Counter("fraud.checks.total")
	flaggedTotal, _ = m.Int64Counter("fraud.flagged.total")
	scoreHist, _    = m.Float64Histogram("fraud.score.distribution",
		metric.WithExplicitBucketBoundaries(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 1.0))
	ruleLatency, _ = m.Float64Histogram("fraud.rule.duration_seconds")

	mux := http.NewServeMux()
	mux.Handle("/health", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "fraud-svc"})
	}))
	mux.Handle("/check", otelhttp.NewHandler(http.HandlerFunc(checkHandler), "fraud.check"))

	fLog.Info("fraud-svc starting", "addr", ":8081")
	if err := http.ListenAndServe(":8081", mux); err != nil {
		fLog.Error("fraud-svc crashed", "err", err)
		os.Exit(1)
	}
}

func checkHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	span := trace.SpanFromContext(ctx)

	var req struct {
		TxID   string  `json:"tx_id"`
		Amount float64 `json:"amount"`
		From   string  `json:"from"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	fLog.InfoContext(ctx, "fraud evaluation started",
		"tx_id", req.TxID, "amount", req.Amount, "from_account", req.From,
		"rules", []string{"large_amount", "bad_actor", "velocity", "ml_model"})

	span.SetAttributes(
		attribute.String("tx.id", req.TxID),
		attribute.Float64("tx.amount", req.Amount),
		attribute.String("tx.from", req.From),
	)

	score := runRules(ctx, req.TxID, req.Amount, req.From)
	flagged := score > 0.75

	span.SetAttributes(attribute.Float64("fraud.final_score", score), attribute.Bool("fraud.flagged", flagged))
	checksTotal.Add(ctx, 1)
	scoreHist.Record(ctx, score)

	if flagged {
		reason := classifyReason(score, req.Amount, req.From)
		flaggedTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
		span.SetStatus(codes.Error, "fraud detected")
		fLog.WarnContext(ctx, "FRAUD DETECTED — transaction blocked",
			"tx_id", req.TxID, "final_score", score,
			"amount", req.Amount, "from_account", req.From, "reason", reason)
	} else {
		span.SetStatus(codes.Ok, "clean")
		fLog.InfoContext(ctx, "fraud evaluation clean", "tx_id", req.TxID, "final_score", score)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"tx_id": req.TxID, "score": score, "flagged": flagged})
}

func runRules(ctx context.Context, txID string, amount float64, from string) float64 {
	score := 0.0
	score += runRule(ctx, txID, "large_amount", func() (float64, []any) {
		if amount > 8000 {
			return 0.40, []any{"threshold", 8000, "actual", amount, "triggered", true}
		}
		return 0.0, []any{"threshold", 8000, "actual", amount, "triggered", false}
	})
	score += runRule(ctx, txID, "bad_actor", func() (float64, []any) {
		if badActors[from] {
			return 0.45, []any{"account", from, "listed", true}
		}
		return 0.0, []any{"account", from, "listed", false}
	})
	score += runRule(ctx, txID, "velocity", func() (float64, []any) {
		if rand.Float64() < 0.15 {
			n := rand.Intn(10) + 5
			return 0.25, []any{"txn_count_1h", n, "threshold", 5, "triggered", true}
		}
		return 0.0, []any{"triggered", false}
	})
	score += runRule(ctx, txID, "ml_model", func() (float64, []any) {
		ml := rand.Float64() * 0.20
		return ml, []any{"model_version", "v2.1.3", "ml_score", ml}
	})
	if score > 1.0 {
		score = 1.0
	}
	return score
}

func runRule(ctx context.Context, txID, name string, fn func() (float64, []any)) float64 {
	rCtx, span := otel.Tracer("banksim.fraud-svc").Start(ctx, "fraud.rule."+name)
	defer span.End()
	start := time.Now()
	time.Sleep(time.Duration(rand.Intn(8)+2) * time.Millisecond)

	contribution, fields := fn()
	duration := time.Since(start).Seconds()

	span.SetAttributes(
		attribute.String("rule.name", name),
		attribute.Float64("rule.score_contribution", contribution),
	)
	ruleLatency.Record(rCtx, duration, metric.WithAttributes(attribute.String("rule", name)))

	args := []any{"rule", name, "tx_id", txID, "score_contribution", contribution}
	args = append(args, fields...)
	if contribution > 0 {
		fLog.WarnContext(rCtx, "fraud rule triggered", args...)
		span.SetStatus(codes.Error, "triggered")
	} else {
		fLog.InfoContext(rCtx, "fraud rule passed", args...)
	}
	return contribution
}

func classifyReason(score float64, amount float64, from string) string {
	if badActors[from] { return "known_bad_actor" }
	if amount > 8000   { return "large_amount" }
	if score > 0.9     { return "composite_high_risk" }
	return "ml_model_flag"
}

func initOTEL(ctx context.Context, svcName string) func(context.Context) {
	endpoint := envOr("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")
	res, _ := resource.New(ctx, resource.WithAttributes(
		semconv.ServiceName(svcName),
		semconv.ServiceVersion("1.0.0"),
		semconv.DeploymentEnvironment("local"),
	))
	traceExp, _ := otlptracegrpc.New(ctx, otlptracegrpc.WithInsecure(), otlptracegrpc.WithEndpoint(endpoint))
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp), sdktrace.WithResource(res), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))
	metricExp, _ := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithInsecure(), otlpmetricgrpc.WithEndpoint(endpoint))
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp, sdkmetric.WithInterval(10*time.Second))),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)
	return func(ctx context.Context) { tp.Shutdown(ctx); mp.Shutdown(ctx) }
}

func envOr(k, d string) string { if v := os.Getenv(k); v != "" { return v }; return d }
