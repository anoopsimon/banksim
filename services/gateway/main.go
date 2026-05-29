package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
	gTracer     trace.Tracer
	gMeter      metric.Meter
	gLog        *slog.Logger
	gHTTPClient *http.Client
	reqTotal    metric.Int64Counter
	errTotal    metric.Int64Counter
	latencyHist metric.Float64Histogram
	activeReqs  metric.Int64UpDownCounter
)

func main() {
	ctx := context.Background()
	shutdown := initOTEL(ctx, "gateway")
	defer shutdown(ctx)

	gTracer = otel.Tracer("banksim.gateway")
	gMeter = otel.Meter("banksim.gateway")
	gLog = otelslog.NewLogger("gateway")
	gHTTPClient = &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
		Timeout:   5 * time.Second,
	}

	reqTotal, _ = gMeter.Int64Counter("gateway.requests.total")
	errTotal, _ = gMeter.Int64Counter("gateway.errors.total")
	latencyHist, _ = gMeter.Float64Histogram("gateway.request.duration_seconds",
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5))
	activeReqs, _ = gMeter.Int64UpDownCounter("gateway.requests.inflight")

	fraudAddr := envOr("FRAUD_SVC_ADDR", "http://fraud-svc:8081")
	ledgerAddr := envOr("LEDGER_SVC_ADDR", "http://ledger-svc:8082")

	mux := http.NewServeMux()
	mux.Handle("/health", http.HandlerFunc(healthHandler))
	mux.Handle("/v1/transfer", otelhttp.NewHandler(http.HandlerFunc(makeTransferHandler(fraudAddr, ledgerAddr)), "transfer"))
	mux.Handle("/v1/balance", otelhttp.NewHandler(http.HandlerFunc(balanceHandler), "balance"))
	mux.Handle("/v1/transactions", otelhttp.NewHandler(http.HandlerFunc(txHistoryHandler), "transactions"))

	gLog.Info("gateway starting", "addr", ":8080", "fraud_svc", fraudAddr, "ledger_svc", ledgerAddr)
	if err := http.ListenAndServe(":8080", loggingMiddleware(mux)); err != nil {
		gLog.Error("gateway crashed", "err", err)
		os.Exit(1)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]string{"status": "ok", "service": "gateway"})
}

func makeTransferHandler(fraudAddr, ledgerAddr string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		span := trace.SpanFromContext(ctx)
		start := time.Now()

		activeReqs.Add(ctx, 1)
		defer activeReqs.Add(ctx, -1)

		var req struct {
			From     string  `json:"from_account"`
			To       string  `json:"to_account"`
			Amount   float64 `json:"amount"`
			Currency string  `json:"currency"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		if req.From == "" {
			req.From, req.To = randAccount(), randAccount()
			req.Amount = randAmount()
			req.Currency = "AUD"
		}

		txID := newTxID()
		span.SetAttributes(
			attribute.String("tx.id", txID),
			attribute.String("tx.from", req.From),
			attribute.String("tx.to", req.To),
			attribute.Float64("tx.amount", req.Amount),
		)
		gLog.InfoContext(ctx, "transfer initiated",
			"tx_id", txID, "from", req.From, "to", req.To,
			"amount", req.Amount, "currency", req.Currency)

		fraudResp, err := callJSON(ctx, fraudAddr+"/check", map[string]any{
			"tx_id": txID, "amount": req.Amount, "from": req.From,
		})
		if err != nil {
			gLog.ErrorContext(ctx, "fraud-svc unreachable", "tx_id", txID, "err", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, "fraud-svc down")
			errTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", "fraud_svc_down")))
			recLatency(ctx, start, "transfer", 503)
			writeJSON(w, 503, errBody("fraud service unavailable"))
			return
		}

		fraudScore, _ := fraudResp["score"].(float64)
		isFraud := fraudScore > 0.75
		span.SetAttributes(attribute.Float64("fraud.score", fraudScore), attribute.Bool("fraud.flagged", isFraud))

		if isFraud {
			gLog.WarnContext(ctx, "transfer BLOCKED — fraud detected",
				"tx_id", txID, "fraud_score", fraudScore, "from", req.From, "amount", req.Amount)
			span.SetStatus(codes.Error, "fraud blocked")
			errTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", "fraud_detected")))
			reqTotal.Add(ctx, 1, metric.WithAttributes(
				attribute.String("endpoint", "transfer"), attribute.String("outcome", "fraud_blocked")))
			recLatency(ctx, start, "transfer", 403)
			writeJSON(w, 403, errBody("transaction blocked: fraud detected"))
			return
		}

		_, err = callJSON(ctx, ledgerAddr+"/commit", map[string]any{
			"tx_id": txID, "from": req.From, "to": req.To,
			"amount": req.Amount, "currency": req.Currency,
		})
		if err != nil {
			gLog.ErrorContext(ctx, "ledger-svc commit failed", "tx_id", txID, "err", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, "ledger error")
			errTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", "ledger_error")))
			recLatency(ctx, start, "transfer", 500)
			writeJSON(w, 500, errBody("ledger commit failed"))
			return
		}

		gLog.InfoContext(ctx, "transfer completed",
			"tx_id", txID, "from", req.From, "to", req.To,
			"amount", req.Amount, "fraud_score", fraudScore,
			"duration_ms", time.Since(start).Milliseconds())
		span.SetStatus(codes.Ok, "")
		reqTotal.Add(ctx, 1, metric.WithAttributes(
			attribute.String("endpoint", "transfer"), attribute.String("outcome", "success")))
		recLatency(ctx, start, "transfer", 200)
		writeJSON(w, 200, map[string]any{"tx_id": txID, "status": "completed", "fraud_score": fraudScore})
	}
}

func balanceHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	span := trace.SpanFromContext(ctx)
	start := time.Now()
	account := r.URL.Query().Get("account")
	if account == "" {
		account = randAccount()
	}
	gLog.InfoContext(ctx, "balance enquiry", "account", account)
	span.SetAttributes(attribute.String("account.id", account))
	time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
	balance := rand.Float64() * 50000
	gLog.InfoContext(ctx, "balance returned", "account", account, "balance_aud", balance)
	reqTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("endpoint", "balance"), attribute.String("outcome", "success")))
	recLatency(ctx, start, "balance", 200)
	writeJSON(w, 200, map[string]any{"account": account, "balance": balance, "currency": "AUD"})
}

func txHistoryHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	span := trace.SpanFromContext(ctx)
	start := time.Now()
	account := r.URL.Query().Get("account")
	if account == "" {
		account = randAccount()
	}
	gLog.InfoContext(ctx, "tx history requested", "account", account)
	span.SetAttributes(attribute.String("account.id", account))
	count := rand.Intn(15) + 1
	txns := make([]map[string]any, count)
	for i := range txns {
		txns[i] = map[string]any{
			"tx_id":  newTxID(),
			"amount": randAmount(),
			"type":   pickOne("debit", "credit"),
			"status": pickOne("completed", "completed", "pending"),
			"date":   time.Now().Add(-time.Duration(rand.Intn(30)) * 24 * time.Hour).Format(time.RFC3339),
		}
	}
	gLog.InfoContext(ctx, "tx history returned", "account", account, "count", count)
	reqTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("endpoint", "transactions"), attribute.String("outcome", "success")))
	recLatency(ctx, start, "transactions", 200)
	writeJSON(w, 200, map[string]any{"account": account, "transactions": txns})
}

func initOTEL(ctx context.Context, svcName string) func(context.Context) {
	endpoint := envOr("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")
	res, _ := resource.New(ctx, resource.WithAttributes(
		semconv.ServiceName(svcName),
		semconv.ServiceVersion("1.0.0"),
		semconv.DeploymentEnvironment("local"),
	))
	traceExp, _ := otlptracegrpc.New(ctx,
		otlptracegrpc.WithInsecure(), otlptracegrpc.WithEndpoint(endpoint))
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))
	metricExp, _ := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithInsecure(), otlpmetricgrpc.WithEndpoint(endpoint))
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp,
			sdkmetric.WithInterval(10*time.Second))),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)
	return func(ctx context.Context) { tp.Shutdown(ctx); mp.Shutdown(ctx) }
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusRecorder{ResponseWriter: w, status: 200}
		start := time.Now()
		next.ServeHTTP(sw, r)
		gLog.InfoContext(r.Context(), "http",
			"method", r.Method, "path", r.URL.Path,
			"status", sw.status, "duration_ms", time.Since(start).Milliseconds())
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (sr *statusRecorder) WriteHeader(c int) { sr.status = c; sr.ResponseWriter.WriteHeader(c) }

func callJSON(ctx context.Context, url string, payload map[string]any) (map[string]any, error) {
	b, _ := json.Marshal(payload)
	req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := gHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		return nil, fmt.Errorf("upstream %s: %d", url, resp.StatusCode)
	}
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return out, nil
}

func recLatency(ctx context.Context, start time.Time, endpoint string, code int) {
	latencyHist.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.String("endpoint", endpoint),
			attribute.Int("http.status_code", code),
		))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func errBody(msg string) map[string]string { return map[string]string{"error": msg} }
func envOr(k, d string) string             { if v := os.Getenv(k); v != "" { return v }; return d }
func newTxID() string                      { return fmt.Sprintf("TXN-%d-%04d", time.Now().UnixMilli(), rand.Intn(9999)) }
func randAccount() string                  { return fmt.Sprintf("ACC%04d", rand.Intn(9999)) }
func randAmount() float64                  { return float64(rand.Intn(9000)+100) + rand.Float64() }
func pickOne(opts ...string) string        { return opts[rand.Intn(len(opts))] }
