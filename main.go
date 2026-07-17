package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/vyuvaraj/ServShared"
	"gopkg.in/yaml.v3"
	"servlock/pkg/handlers"
	"servlock/pkg/storage"
)

type Config struct {
	Port     string `yaml:"port"`
	Backend  string `yaml:"backend"`
	FilePath string `yaml:"file_path"`
	APIKey   string `yaml:"api_key"`
	TLSCert  string `yaml:"tls_cert"`
	TLSKey   string `yaml:"tls_key"`
	ClientCA string `yaml:"client_ca"`
}

func main() {
	configPath := flag.String("config", "", "Path to servlock.yaml config file")
	portFlag := flag.String("port", "8089", "Port to listen on (override)")
	flag.Parse()

	var cfg Config

	if *configPath != "" {
		cfgData, err := os.ReadFile(*configPath)
		if err != nil {
			log.Fatalf("failed to read config file: %v", err)
		}
		if err := yaml.Unmarshal(cfgData, &cfg); err != nil {
			log.Fatalf("failed to parse yaml config: %v", err)
		}
	} else {
		// Default config values
		cfg = Config{
			Port:     *portFlag,
			Backend:  "memory",
			FilePath: "leases.json",
			APIKey:   os.Getenv("SERVLOCK_API_KEY"),
		}
	}

	log.Printf("Starting ServLock Distributed Lock Manager on port %s (backend: %s)...", cfg.Port, cfg.Backend)

	// Initialize Backend Storage
	if cfg.Backend == "file" {
		store, err := storage.NewFileLockStore(cfg.FilePath)
		if err != nil {
			log.Fatalf("failed to initialize FileLockStore: %v", err)
		}
		handlers.Store = store
	} else {
		handlers.Store = storage.NewInMemoryStore()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", ServShared.HealthzHandler)
	mux.HandleFunc("/readyz", ServShared.ReadyzHandler)
	mux.HandleFunc("/api/version", ServShared.VersionHandler("servlock", "1.0.0"))
	mux.HandleFunc("/api/v1/version", ServShared.VersionHandler("servlock", "1.0.0"))

	mux.HandleFunc("/api/locks/acquire", handlers.HandleAcquireLock)
	mux.HandleFunc("/api/locks/release", handlers.HandleReleaseLock)
	mux.HandleFunc("/api/locks/renew", handlers.HandleRenewLock)
	mux.HandleFunc("/api/locks/observability", handlers.HandleLockObservability)
	mux.HandleFunc("/api/locks/metrics", handlers.HandleMetrics)
	mux.HandleFunc("/api/locks/subscribe", handlers.HandleLockSubscribe)
	mux.HandleFunc("/api/locks/heartbeat", handlers.HandleHeartbeatPing)

	// Wrapper handler for /api/v1/ prefix rewriting (V1.1 support)
	v1Wrapper := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v1/") {
			r.URL.Path = "/api/" + strings.TrimPrefix(r.URL.Path, "/api/v1/")
		}
		mux.ServeHTTP(w, r)
	})

	var serverHandler http.Handler = v1Wrapper

	// If API Key is configured, use API Key auth. Otherwise fallback to standard AuthMiddleware.
	if cfg.APIKey != "" {
		log.Println("API Key protection enabled (zero-dependency standalone mode).")
		apiKeyAuth := func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
					next.ServeHTTP(w, r)
					return
				}
				key := r.Header.Get("X-API-Key")
				if key == "" {
					authHeader := r.Header.Get("Authorization")
					if strings.HasPrefix(authHeader, "Bearer ") {
						key = strings.TrimPrefix(authHeader, "Bearer ")
					}
				}
				if key != cfg.APIKey {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusUnauthorized)
					w.Write([]byte(`{"error": "Unauthorized: invalid or missing API key"}`))
					return
				}
				next.ServeHTTP(w, r)
			})
		}
		serverHandler = apiKeyAuth(v1Wrapper)
	} else {
		// Standard ecosystem auth chain
		rateLimiter := ServShared.RateLimitMiddleware
		if flag.Lookup("test.v") != nil {
			rateLimiter = func(next http.Handler) http.Handler {
				return next
			}
		}
		serverHandler = ServShared.TraceMiddleware("servlock",
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
	}

	server := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: serverHandler,
	}

	// Set up TLS/mTLS configuration if certs are provided
	if cfg.TLSCert != "" && cfg.TLSKey != "" {
		tlsConfig := &tls.Config{}
		if cfg.ClientCA != "" {
			caCert, err := os.ReadFile(cfg.ClientCA)
			if err != nil {
				log.Fatalf("failed to read client CA file: %v", err)
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			tlsConfig.ClientCAs = caCertPool
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
			log.Println("mTLS client verification enabled.")
		}
		server.TLSConfig = tlsConfig
	}

	go func() {
		if server.TLSConfig != nil {
			log.Printf("Starting HTTPS server on port %s...", cfg.Port)
			if err := server.ListenAndServeTLS(cfg.TLSCert, cfg.TLSKey); err != nil && err != http.ErrServerClosed {
				log.Fatalf("ListenAndServeTLS failed: %v", err)
			}
		} else {
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("ListenAndServe failed: %v", err)
			}
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
