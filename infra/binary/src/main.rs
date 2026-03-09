use std::net::SocketAddr;

use clap::Parser;

/// TraceHouse — single-binary distribution with embedded frontend and CORS proxy.
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    /// Port to listen on
    #[arg(short, long, default_value_t = 8990, env = "CLICKHOUSE_MONITOR_PORT")]
    port: u16,

    /// Skip TLS certificate verification for upstream ClickHouse connections
    #[arg(long, env = "CLICKHOUSE_MONITOR_INSECURE")]
    insecure: bool,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let app = clickhouse_monitor::build_router_with(clickhouse_monitor::RouterOptions {
        insecure: cli.insecure,
    });

    let addr = SocketAddr::from(([0, 0, 0, 0], cli.port));
    println!("TraceHouse listening on http://localhost:{}", cli.port);
    println!("  App:    http://localhost:{}/", cli.port);
    println!("  Proxy:  http://localhost:{}/proxy/query", cli.port);
    println!("  Health: http://localhost:{}/health", cli.port);
    if cli.insecure {
        println!("  TLS:    certificate verification DISABLED");
    }

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.ok();
            println!("\nShutting down...");
        })
        .await
        .unwrap();
}
