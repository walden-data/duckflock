use clap::Parser;

/// DuckFlock — a persistent PostgreSQL-compatible endpoint for DuckLake.
#[derive(Parser, Debug)]
#[command(name = "duckflock", version, about)]
struct Cli {
    /// Path to configuration file
    #[arg(short, long, default_value = "duckflock.yaml")]
    config: String,

    /// Override log level
    #[arg(long)]
    log_level: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(cli.log_level.as_deref().unwrap_or("info"))
        .init();

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        config = %cli.config,
        "Starting DuckFlock"
    );

    // Load config
    let config = duckflock_core::config::DuckFlockConfig::from_file(&cli.config)?;

    tracing::info!(
        host = %config.listen.host,
        port = %config.listen.port,
        catalogs = config.catalogs.len(),
        "Configuration loaded"
    );

    // TODO: Initialize engine (DF-9)
    // TODO: Initialize gateway (DF-3)
    // TODO: Start serving

    tracing::info!(
        "DuckFlock ready — listening on {}:{}",
        config.listen.host,
        config.listen.port
    );

    // Keep running until signal
    tokio::signal::ctrl_c().await?;
    tracing::info!("Shutting down");

    Ok(())
}
