package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/vyuvaraj/ServShared"
	"servlock/pkg/handlers"
	"servlock/pkg/storage"
)

func main() {
	port := flag.String("port", "8089", "Port to listen on")
	flag.Parse()

	log.Printf("Starting ServLock Distributed Lock Manager on port %s...", *port)

	// Initialize fallback local memory storage
	handlers.Store = storage.NewInMemoryStore()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", ServShared.HealthzHandler)
	mux.HandleFunc("/readyz", ServShared.ReadyzHandler)
	mux.HandleFunc("/api/version", ServShared.VersionHandler("servlock", "1.0.0"))
	mux.HandleFunc("/api/v1/version", ServShared.VersionHandler("servlock", "1.0.0"))

	mux.HandleFunc("/api/locks/acquire", handlers.HandleAcquireLock)
	mux.HandleFunc("/api/locks/release", handlers.HandleReleaseLock)
	mux.HandleFunc("/api/locks/renew", handlers.HandleRenewLock)
	mux.HandleFunc("/api/locks/observability", handlers.HandleLockObservability)

	// Wrapper handler for /api/v1/ prefix rewriting (V1.1 support)
	v1Wrapper := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v1/") {
			r.URL.Path = "/api/" + strings.TrimPrefix(r.URL.Path, "/api/v1/")
		}
		mux.ServeHTTP(w, r)
	})

	rateLimiter := ServShared.RateLimitMiddleware
	if flag.Lookup("test.v") != nil {
		rateLimiter = func(next http.Handler) http.Handler {
			return next
		}
	}

	// Wrap in ServShared middleware: Trace -> RateLimit -> CORS -> MaxBytes -> Auth -> Tenant -> v1Wrapper
	serverHandler := ServShared.TraceMiddleware("servlock",
		rateLimiter(
			ServShared.CORSMiddleware(
				ServShared.MaxBytesMiddleware(10*1024*1024)(
					ServShared.AuthMiddleware(
						ServShared.TenantMiddleware(v1Wrapper),
					),
				),
			),
		),
	)

	server := &http.Server{
		Addr:    ":" + *port,
		Handler: serverHandler,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe failed: %v", err)
		}
	}()

	log.Printf("ServLock is ready to accept lease requests.")

	// Await signal for graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down ServLock server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("ServLock stopped cleanly.")
}
