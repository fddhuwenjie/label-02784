pub mod handler;
pub mod rate_limit;
pub mod state;

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Response, Server, StatusCode};
use tokio::sync::watch;
use tracing::{error, info, warn};

use self::rate_limit::RateLimiter;
use self::state::HealthState;

/// Production-grade HTTP server for health checks, metrics, and mock LLM.
///
/// Improvements over the original raw-TCP implementation:
/// - Proper HTTP/1.1 parsing via `hyper` (chunked encoding, keep-alive, pipelining)
/// - Strict method-based routing (GET-only for read endpoints, POST-only for mock-llm)
/// - Per-IP token-bucket rate limiting with automatic stale-entry eviction
/// - Security response headers (`X-Content-Type-Options`, `X-Frame-Options`, `Cache-Control`)
/// - Request body size limit enforcement
/// - Header-read timeout to mitigate slowloris attacks
/// - Graceful shutdown integrated with the service-wide shutdown signal
pub struct HttpServer {
    port: u16,
    state: Arc<HealthState>,
    shutdown_rx: watch::Receiver<bool>,
}

impl HttpServer {
    pub fn new(
        port: u16,
        state: Arc<HealthState>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            port,
            state,
            shutdown_rx,
        }
    }

    pub async fn run(self) {
        let port = self.port;
        let shutdown_rx = self.shutdown_rx;
        let addr = SocketAddr::from(([0, 0, 0, 0], port));

        // Token-bucket: 60 burst capacity, refills at 10 req/s per IP
        let rate_limiter = Arc::new(RateLimiter::new(60, 10.0));

        // Periodic eviction of stale rate-limit entries (every 5 min, idle > 10 min)
        let rl_cleanup = rate_limiter.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(300));
            loop {
                interval.tick().await;
                rl_cleanup.evict_idle(600);
            }
        });

        let state = self.state;
        let limiter = rate_limiter;

        let make_svc = make_service_fn(move |conn: &AddrStream| {
            let state = state.clone();
            let limiter = limiter.clone();
            let remote_addr = conn.remote_addr();

            async move {
                Ok::<_, Infallible>(service_fn(move |req| {
                    let state = state.clone();
                    let limiter = limiter.clone();

                    async move {
                        // Rate limiting check
                        if !limiter.allow(remote_addr.ip()) {
                            state
                                .metrics
                                .rate_limited_total
                                .fetch_add(1, Ordering::Relaxed);
                            warn!(client = %remote_addr, "Rate limited");
                            return Ok::<_, Infallible>(
                                Response::builder()
                                    .status(StatusCode::TOO_MANY_REQUESTS)
                                    .header("Content-Type", "application/json")
                                    .header("Retry-After", "1")
                                    .body(Body::from(r#"{"error":"rate_limited"}"#))
                                    .unwrap(),
                            );
                        }

                        Ok::<_, Infallible>(handler::route(req, state).await)
                    }
                }))
            }
        });

        let incoming = match hyper::server::conn::AddrIncoming::bind(&addr) {
            Ok(inc) => {
                info!(port, "HTTP server listening");
                inc
            }
            Err(e) => {
                error!(port, error = %e, "Failed to bind HTTP server port");
                return;
            }
        };

        let server = Server::builder(incoming)
            .http1_keepalive(true)
            .http1_header_read_timeout(Duration::from_secs(5))
            .serve(make_svc);

        let mut shutdown_rx = shutdown_rx;
        let graceful = server.with_graceful_shutdown(async move {
            loop {
                if shutdown_rx.changed().await.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
            info!("HTTP server received shutdown signal");
        });

        if let Err(e) = graceful.await {
            error!(error = %e, "HTTP server error");
        }

        info!("HTTP server shut down");
    }
}
