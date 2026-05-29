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
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// ═══════════════════════════════════════════════════════════════
//  LOAD GENERATOR
//  Simulates realistic banking traffic:
//  - Transfers (60%): mix of normal + large + bad-actor accounts
//  - Balance checks (25%)
//  - Transaction history (15%)
//  - Concurrency: 5 workers, ~2–8 req/s per worker
// ═══════════════════════════════════════════════════════════════

var log = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

const (
	workers     = 5
	gatewayAddr = "http://gateway:8080"
)

// Realistic account pool — includes known bad actors from fraud-svc
var accounts = []string{
	"ACC1001", "ACC1002", "ACC1003", "ACC1004", "ACC1005",
	"ACC2001", "ACC2002", "ACC2003", "ACC2004", "ACC2005",
	"ACC3001", "ACC3002", "ACC3003",
	// Bad actors (will trigger fraud)
	"ACC0013", "ACC0042", "ACC0666",
}

type stats struct {
	mu       sync.Mutex
	total    int64
	success  int64
	fraud    int64
	errors   int64
	start    time.Time
}

func (s *stats) record(code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.total++
	switch {
	case code == 200: s.success++
	case code == 403: s.fraud++
	default:          s.errors++
	}
}

func (s *stats) print() {
	s.mu.Lock()
	defer s.mu.Unlock()
	elapsed := time.Since(s.start).Seconds()
	rps := float64(s.total) / elapsed
	log.Info("loadgen stats",
		"total", s.total,
		"success", s.success,
		"fraud_blocked", s.fraud,
		"errors", s.errors,
		"rps", fmt.Sprintf("%.2f", rps),
		"elapsed_s", fmt.Sprintf("%.0f", elapsed),
	)
}

var globalStats = &stats{start: time.Now()}

func main() {
	gatewayURL := envOr("GATEWAY_ADDR", gatewayAddr)

	log.Info("loadgen starting",
		"gateway", gatewayURL,
		"workers", workers,
	)

	// Wait for gateway to be ready
	waitForGateway(gatewayURL)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var wg sync.WaitGroup

	// Launch worker pool
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runWorker(ctx, workerID, gatewayURL)
		}(i)
	}

	// Stats printer
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C: globalStats.print()
			case <-ctx.Done(): return
			}
		}
	}()

	<-ctx.Done()
	log.Info("loadgen shutting down")
	wg.Wait()
	globalStats.print()
}

func runWorker(ctx context.Context, id int, gateway string) {
	log.Info("worker started", "worker_id", id)
	for {
		select {
		case <-ctx.Done():
			log.Info("worker stopped", "worker_id", id)
			return
		default:
		}

		// Weighted operation selection
		n := rand.Float64()
		var (code int; err error)
		switch {
		case n < 0.60: code, err = doTransfer(ctx, gateway)
		case n < 0.85: code, err = doBalance(ctx, gateway)
		default:       code, err = doTxHistory(ctx, gateway)
		}

		if err != nil {
			log.Error("request failed", "worker_id", id, "err", err)
			globalStats.record(500)
		} else {
			globalStats.record(code)
		}

		// Randomised sleep: 100–600ms between requests per worker
		time.Sleep(time.Duration(100+rand.Intn(500)) * time.Millisecond)
	}
}

func doTransfer(ctx context.Context, gateway string) (int, error) {
	from := pickAccount()
	to   := pickAccount()
	for to == from { to = pickAccount() }

	// ~10% of transfers are large amounts (triggers fraud rule)
	amount := randAmount()
	if rand.Float64() < 0.10 { amount = 8500 + rand.Float64()*3000 }

	body, _ := json.Marshal(map[string]any{
		"from_account": from,
		"to_account":   to,
		"amount":       amount,
		"currency":     "AUD",
	})

	req, _ := http.NewRequestWithContext(ctx, "POST", gateway+"/v1/transfer", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Loadgen", "true")

	resp, err := http.DefaultClient.Do(req)
	if err != nil { return 0, err }
	resp.Body.Close()

	log.Info("transfer sent",
		"from", from, "to", to,
		"amount", amount, "status", resp.StatusCode,
	)
	return resp.StatusCode, nil
}

func doBalance(ctx context.Context, gateway string) (int, error) {
	account := pickAccount()
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s/v1/balance?account=%s", gateway, account), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil { return 0, err }
	resp.Body.Close()
	return resp.StatusCode, nil
}

func doTxHistory(ctx context.Context, gateway string) (int, error) {
	account := pickAccount()
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s/v1/transactions?account=%s", gateway, account), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil { return 0, err }
	resp.Body.Close()
	return resp.StatusCode, nil
}

func waitForGateway(addr string) {
	log.Info("waiting for gateway to be ready...")
	for {
		resp, err := http.Get(addr + "/health")
		if err == nil && resp.StatusCode == 200 {
			log.Info("gateway is ready")
			return
		}
		log.Info("gateway not ready yet, retrying in 2s")
		time.Sleep(2 * time.Second)
	}
}

func pickAccount() string { return accounts[rand.Intn(len(accounts))] }
func randAmount() float64 { return float64(rand.Intn(5000)) + rand.Float64()*1000 }
func envOr(k, d string) string { if v := os.Getenv(k); v != "" { return v }; return d }
