package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"sync"
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

// ═══════════════════════════════════════════════════════════════
//  LEDGER-SVC
//  Receives: POST /commit  { tx_id, from, to, amount, currency }
//  Simulates: debit → credit → audit_log  (3 fake "DB" calls)
//  Tracks:  db query duration, connection pool wait, commit count
// ═══════════════════════════════════════════════════════════════

var (
	tracer          trace.Tracer
	meter           metric.Meter
	log             *slog.Logger
	commitsTotal    metric.Int64Counter
	dbQueryDuration metric.Float64Histogram
	dbPoolWait      metric.Float64Histogram
	dbPoolActive    metric.Int64UpDownCounter
	// Fake in-memory ledger (account → balance)
	ledger   = map[string]float64{}
	ledgerMu sync.RWMutex
)

const (
	dbPoolSize = 10 // fake connection pool size
)

func main() {
	ctx := context.Background()
	shutdown := initOTEL(ctx, "ledger-svc")
	defer shutdown(ctx)

	tracer = otel.Tracer("banksim.ledger-svc")
	meter  = otel.Meter("banksim.ledger-svc")
	log    = otelslog.NewLogger("ledger-svc")

	commitsTotal, _ = meter.Int64Counter("ledger.commits.total",
		metric.WithDescription("Total ledger commits"),
	)
	dbQueryDuration, _ = meter.Float64Histogram("ledger.db.query_duration_seconds",
		metric.WithDescription("DB query latency per operation"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25),
	)
	dbPoolWait, _ = meter.Float64Histogram("ledger.db.pool_wait_seconds",
		metric.WithDescription("Time waiting for a DB connection from pool"),
	)
	dbPoolActive, _ = meter.Int64UpDownCounter("ledger.db.pool_active",
		metric.WithDescription("Active DB connections from pool"),
	)

	mux := http.NewServeMux()
	mux.Handle("/health", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "ledger-svc"})
	}))
	mux.Handle("/commit", otelhttp.NewHandler(http.HandlerFunc(commitHandler), "ledger.commit"))

	log.Info("ledger-svc starting", "addr", ":8082", "db_pool_size", dbPoolSize)
	if err := http.ListenAndServe(":8082", mux); err != nil {
		log.Error("ledger-svc crashed", "err", err); os.Exit(1)
	}
}

func commitHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	span := trace.SpanFromContext(ctx)

	var req struct {
		TxID     string  `json:"tx_id"`
		From     string  `json:"from"`
		To       string  `json:"to"`
		Amount   float64 `json:"amount"`
		Currency string  `json:"currency"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	log.InfoContext(ctx, "ledger commit started",
		"tx_id", req.TxID,
		"from", req.From,
		"to", req.To,
		"amount", req.Amount,
		"currency", req.Currency,
	)
	span.SetAttributes(
		attribute.String("tx.id", req.TxID),
		attribute.String("tx.from", req.From),
		attribute.String("tx.to", req.To),
		attribute.Float64("tx.amount", req.Amount),
	)

	// Acquire fake DB connection (pool wait simulation)
	poolStart := time.Now()
	poolWait := time.Duration(rand.Intn(15)) * time.Millisecond
	time.Sleep(poolWait)
	dbPoolWait.Record(ctx, time.Since(poolStart).Seconds())
	dbPoolActive.Add(ctx, 1)
	defer dbPoolActive.Add(ctx, -1)

	log.InfoContext(ctx, "db connection acquired",
		"tx_id", req.TxID,
		"wait_ms", poolWait.Milliseconds(),
		"pool_size", dbPoolSize,
	)

	// ── Step 1: Debit from account ──────────────────────────────────────────
	if err := dbOp(ctx, "UPDATE", "accounts", "debit_source",
		func() error {
			ledgerMu.Lock()
			defer ledgerMu.Unlock()
			bal := ledger[req.From]
			if bal == 0 { bal = 50000 + rand.Float64()*50000 } // seed new accounts
			newBal := bal - req.Amount
			ledger[req.From] = newBal
			log.InfoContext(ctx, "debit applied",
				"tx_id", req.TxID, "account", req.From,
				"previous_balance", bal, "new_balance", newBal, "amount", req.Amount,
			)
			return nil
		}); err != nil {
		respondError(ctx, w, span, req.TxID, "debit failed", err)
		return
	}

	// ── Step 2: Credit to account ───────────────────────────────────────────
	if err := dbOp(ctx, "UPDATE", "accounts", "credit_destination",
		func() error {
			ledgerMu.Lock()
			defer ledgerMu.Unlock()
			bal := ledger[req.To]
			if bal == 0 { bal = 50000 + rand.Float64()*50000 }
			newBal := bal + req.Amount
			ledger[req.To] = newBal
			log.InfoContext(ctx, "credit applied",
				"tx_id", req.TxID, "account", req.To,
				"previous_balance", bal, "new_balance", newBal, "amount", req.Amount,
			)
			return nil
		}); err != nil {
		respondError(ctx, w, span, req.TxID, "credit failed", err)
		return
	}

	// ── Step 3: Write audit log ─────────────────────────────────────────────
	if err := dbOp(ctx, "INSERT", "audit_log", "write_audit",
		func() error {
			log.InfoContext(ctx, "audit log written",
				"tx_id", req.TxID, "from", req.From, "to", req.To,
				"amount", req.Amount, "currency", req.Currency,
				"timestamp", time.Now().UTC().Format(time.RFC3339),
			)
			return nil
		}); err != nil {
		respondError(ctx, w, span, req.TxID, "audit log failed", err)
		return
	}

	commitsTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("currency", req.Currency)))
	span.SetStatus(codes.Ok, "committed")

	log.InfoContext(ctx, "ledger commit completed",
		"tx_id", req.TxID, "from", req.From, "to", req.To,
		"amount", req.Amount, "steps", 3,
	)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"tx_id":  req.TxID,
		"status": "committed",
	})
}

// dbOp simulates a DB query: creates a child span, sleeps, records metrics
func dbOp(ctx context.Context, op, table, label string, fn func() error) error {
	dbCtx, span := otel.Tracer("banksim.ledger-svc").Start(ctx, "db."+label,
		trace.WithAttributes(
			attribute.String("db.operation", op),
			attribute.String("db.table", table),
			attribute.String("db.system", "sqlite"),
		),
	)
	defer span.End()

	start := time.Now()
	// Simulate realistic DB latency: mostly fast, occasional slow query
	sleepMs := rand.Intn(20) + 2
	if rand.Float64() < 0.05 { sleepMs = rand.Intn(150) + 80 } // 5% slow queries
	time.Sleep(time.Duration(sleepMs) * time.Millisecond)

	err := fn()
	duration := time.Since(start).Seconds()

	isSlowQuery := sleepMs > 80
	span.SetAttributes(
		attribute.Float64("db.duration_seconds", duration),
		attribute.Bool("db.slow_query", isSlowQuery),
	)

	dbQueryDuration.Record(dbCtx, duration,
		metric.WithAttributes(
			attribute.String("operation", op),
			attribute.String("table", table),
		),
	)

	if isSlowQuery {
		log.WarnContext(dbCtx, "slow DB query detected",
			"operation", op, "table", table,
			"duration_ms", time.Since(start).Milliseconds(),
		)
		span.SetStatus(codes.Ok, "slow but ok")
	}

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

func respondError(ctx context.Context, w http.ResponseWriter, span trace.Span, txID, msg string, err error) {
	log.ErrorContext(ctx, msg, "tx_id", txID, "err", err)
	span.RecordError(err)
	span.SetStatus(codes.Error, msg)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(500)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
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
		sdktrace.WithBatcher(traceExp), sdktrace.WithResource(res), sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	metricExp, _ := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithInsecure(), otlpmetricgrpc.WithEndpoint(endpoint))
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp, sdkmetric.WithInterval(10*time.Second))),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)
	return func(ctx context.Context) { tp.Shutdown(ctx); mp.Shutdown(ctx) }
}

func envOr(k, d string) string { if v := os.Getenv(k); v != "" { return v }; return d }
var _ = fmt.Sprintf
