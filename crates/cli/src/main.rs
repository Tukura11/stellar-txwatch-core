use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tracing::info;
use txwatch_config::AppConfig;
use txwatch_notifier::{build_client, send_webhook, test_payload_with_network};

// ── CLI definition ────────────────────────────────────────────────────────────

const VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("TXWATCH_GIT_SHA"),
    " built ",
    env!("TXWATCH_BUILD_TIMESTAMP"),
    ")"
);

#[derive(Parser)]
#[command(
    name    = "txwatch",
    version = VERSION,
    about   = "Stellar Soroban contract monitor & webhook alert engine"
)]
struct Cli {
    /// Path to the TOML config file
    #[arg(short, long, default_value = "config/example.toml")]
    config: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the polling engine (watches all contracts in the config)
    Watch {
        /// Do not actually send webhooks; only log matched rules
        #[arg(long)]
        dry_run: bool,
    },

    /// Parse and validate the config file, then print a summary
    ///
    /// Exit codes: 0 = valid config, 1 = invalid or missing config
    Validate,

    /// Send a test webhook payload to a URL and exit
    TestWebhook {
        /// The webhook URL to POST to
        #[arg(long)]
        url: String,

        /// Label to include in the test payload
        #[arg(long, default_value = "TxWatch Test")]
        label: String,
    },
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let cli = Cli::parse();

    match cli.command {
        Command::Validate => {
            let cfg = AppConfig::from_file(&cli.config)?;
            println!("Config is valid.");
            println!("  poll_interval_seconds : {}", cfg.poll_interval_seconds);
            println!("  contracts             : {}", cfg.contracts.len());
            println!();
            for c in &cfg.contracts {
                println!(
                    "  [{network}] {label}",
                    network = c.network.display_name(),
                    label = c.label
                );
                println!("    contract_id  : {}", c.contract_id);
                println!("    webhook_url  : {}", c.webhook_url);
                println!(
                    "    secret       : {}",
                    if c.webhook_secret.is_some() {
                        "set"
                    } else {
                        "none"
                    }
                );
                println!("    rules        : {}", c.rules.len());
                println!("    horizon      : {}", c.network.horizon_base_url());
                println!(
                    "    explorer     : {}/contract/{}",
                    c.network.explorer_base_url(),
                    c.contract_id
                );
            }
        }

        Command::TestWebhook { url, label } => {
            let cfg = AppConfig::from_file(&cli.config)?;
            if cfg.contracts.is_empty() {
                return Err(anyhow::anyhow!(
                    "config has no contracts; cannot derive network for test-webhook"
                ));
            }
            let first_contract = &cfg.contracts[0];
            let network_name = first_contract.network.as_str();
            let horizon_base_url = first_contract.network.horizon_base_url();
            let payload = test_payload_with_network(&label, &url, network_name, horizon_base_url);
            let client  = build_client().context("failed to build HTTP client")?;

            info!(url = %url, "sending test webhook");
            send_webhook(&client, &url, &payload, None)
                .await
                .with_context(|| format!("test webhook to '{}' failed", url))?;
            println!("Test webhook delivered successfully to {}", url);
        }

        Command::Watch { dry_run } => {
            let cfg = AppConfig::from_file(&cli.config)?;
            info!(
                version        = VERSION,
                contracts      = cfg.contracts.len(),
                interval_secs  = cfg.poll_interval_seconds,
                dry_run        = dry_run,
                "starting TxWatch"
            );
            txwatch_poller::run_with(cfg, dry_run).await?;
        }
    }

    Ok(())
}

// ── Tracing initialisation ────────────────────────────────────────────────────

fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();
}
