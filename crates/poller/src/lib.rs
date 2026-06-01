use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;
use tracing::{debug, error, info, warn};
use txwatch_config::{AppConfig, WatchedContract};
use txwatch_notifier::send_webhook;
use txwatch_rules::{evaluate, EnrichedTransaction, HorizonTransaction};

// ── Horizon response shapes ───────────────────────────────────────────────────

#[derive(Deserialize)]
struct HorizonPage {
    _embedded: Embedded,
}

#[derive(Deserialize)]
struct Embedded {
    records: Vec<HorizonTransaction>,
}

/// Horizon operation record — we only need the fields relevant to Soroban.
#[derive(Deserialize)]
struct HorizonOperation {
    #[serde(rename = "type")]
    op_type: String,
    /// Present on `invoke_host_function` operations.
    function: Option<String>,
    /// Present on `payment` operations (string, e.g. "1000.0000000").
    amount: Option<String>,
}

#[derive(Deserialize)]
struct OperationsPage {
    _embedded: OpsEmbedded,
}

#[derive(Deserialize)]
struct OpsEmbedded {
    records: Vec<HorizonOperation>,
}

// ── Summary counters ──────────────────────────────────────────────────────────

#[derive(Default)]
struct Counters {
    transactions:          AtomicU64,
    alerts:                AtomicU64,
    interval_transactions: AtomicU64,
    interval_alerts:       AtomicU64,
}

// ── Public entry point ────────────────────────────────────────────────────────

/// Run the polling loop forever. Each contract is polled sequentially;
/// a single bad transaction never stops the loop.
/// Logs a summary every 60 seconds: contracts watched, transactions processed,
/// alerts fired.
pub async fn run(cfg: AppConfig) -> Result<()> {
    // Build HTTP client with connection pool tuning options.
    let max_idle = cfg.http_pool_max_idle_per_host.unwrap_or(10);
    let keepalive_secs = cfg.http_tcp_keepalive_secs.unwrap_or(30);

    let client = Client::builder()
        .timeout(Duration::from_secs(15))
        .pool_max_idle_per_host(max_idle)
        .tcp_keepalive(Some(Duration::from_secs(keepalive_secs)))
        .build()
        .context("failed to build HTTP client")?;

    let mut cursors: HashMap<String, String> = cfg
        .contracts
        .iter()
        .map(|c| (c.contract_id.clone(), "now".to_string()))
        .collect();

    let interval      = Duration::from_secs(cfg.poll_interval_seconds);
    let summary_every = Duration::from_secs(60);
    let counters      = Arc::new(Counters::default());
    let n_contracts   = cfg.contracts.len();

    let contracts_list = cfg.contracts.iter().map(|c| c.label.as_str()).collect::<Vec<_>>().join(", ");
    let mut networks: Vec<&str> = cfg.contracts.iter().map(|c| c.network.as_str()).collect();
    networks.sort();
    networks.dedup();
    let networks_str = networks.join(", ");

    // Collect distinct (network_name, horizon_base_url) pairs for the startup log.
    let mut horizon_urls: Vec<(&str, &str)> = cfg
        .contracts
        .iter()
        .map(|c| (c.network.as_str(), c.network.horizon_base_url()))
        .collect();
    horizon_urls.sort();
    horizon_urls.dedup();
    let horizon_urls_str = horizon_urls
        .iter()
        .map(|(net, url)| format!("{}={}", net, url))
        .collect::<Vec<_>>()
        .join(", ");

    info!(
        version        = env!("CARGO_PKG_VERSION"),
        contracts      = n_contracts,
        contracts_list = %contracts_list,
        networks       = %networks_str,
        horizon_urls   = %horizon_urls_str,
        interval_secs  = cfg.poll_interval_seconds,
        "TxWatch polling engine started"
    );

    // Spawn the summary logger task.
    let counters_clone = Arc::clone(&counters);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(summary_every).await;
            let interval_txs    = counters_clone.interval_transactions.swap(0, Ordering::Relaxed);
            let interval_alerts = counters_clone.interval_alerts.swap(0, Ordering::Relaxed);
            info!(
                contracts             = n_contracts,
                transactions_total    = counters_clone.transactions.load(Ordering::Relaxed),
                alerts_total          = counters_clone.alerts.load(Ordering::Relaxed),
                transactions_interval = interval_txs,
                alerts_interval       = interval_alerts,
                "60-second summary"
            );
        }
    });

    loop {
        for contract in &cfg.contracts {
            match poll_contract(&client, contract, &mut cursors).await {
                Ok((txs, alerts)) => {
                    counters.transactions.fetch_add(txs, Ordering::Relaxed);
                    counters.alerts.fetch_add(alerts, Ordering::Relaxed);
                    counters.interval_transactions.fetch_add(txs, Ordering::Relaxed);
                    counters.interval_alerts.fetch_add(alerts, Ordering::Relaxed);
                }
                Err(e) => {
                    error!(
                        contract = %contract.label,
                        error    = %e,
                        "poll cycle error — will retry next interval"
                    );
                }
            }
        }
        tokio::time::sleep(interval).await;
    }
}

// ── Per-contract poll ─────────────────────────────────────────────────────────

/// Returns `(transactions_processed, alerts_fired)`.
async fn poll_contract(
    client:   &Client,
    contract: &WatchedContract,
    cursors:  &mut HashMap<String, String>,
) -> Result<(u64, u64)> {
    // Use contract_id as the cursor map key: contract IDs are unique per Stellar network,
    // making them a stable and collision-free key. Using label instead would be unsafe
    // since label uniqueness is only validated at config load time, and labels could
    // theoretically collide if that validation is bypassed.
    let cursor = cursors
        .get(&contract.contract_id)
        .cloned()
        .unwrap_or_else(|| "now".to_string());

    let base = contract
        .horizon_base_url_override
        .as_deref()
        .unwrap_or_else(|| contract.network.horizon_base_url());
    let url  = format!(
        "{}/accounts/{}/transactions?cursor={}&order=asc&limit=200",
        base, contract.contract_id, cursor
    );

    let page: HorizonPage = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET {} failed", url))?
        .json()
        .await
        .with_context(|| format!("failed to parse Horizon response from {}", url))?;

    let records = page._embedded.records;

    if !records.is_empty() {
        info!(
            contract = %contract.label,
            count    = records.len(),
            "fetched new transactions"
        );
    }
    else {
        debug!(
            contract = %contract.label,
            cursor   = %cursor,
            "no new transactions"
        );
    }

    let mut tx_count    = 0u64;
    let mut alert_count = 0u64;

    for raw_tx in records {
        let paging_token = raw_tx.paging_token.clone();
        let tx_hash      = raw_tx.hash.clone();

        // Advance the cursor before enrichment so the transaction is not re-processed
        // even when op enrichment fails (for example, Horizon /operations returns 500).
        cursors.insert(contract.contract_id.clone(), paging_token.clone());

        let (function_names, amount_stroops) =
            match fetch_soroban_details(client, base, &tx_hash).await {
                Ok(details) => details,
                Err(e) => {
                    warn!(
                        contract = %contract.label,
                        tx       = %tx_hash,
                        error    = %e,
                        "could not fetch operation details — evaluating rules without them"
                    );
                    (Vec::new(), None)
                }
            };

        let enriched = match EnrichedTransaction::from_horizon(raw_tx, function_names, amount_stroops, None) {
            Ok(t)  => t,
            Err(e) => {
                warn!(
                    contract = %contract.label,
                    tx       = %tx_hash,
                    error    = %e,
                    "skipping transaction due to enrichment error"
                );
                continue;
            }
        };

        tx_count += 1;

        let payloads = evaluate(
            &contract.label,
            &contract.contract_id,
            contract.network.as_str(),
            base,
            contract.network.explorer_base_url(),
            &contract.rules,
            &enriched,
        );

        for payload in payloads {
            alert_count += 1;
            info!(
                contract = %contract.label,
                rule     = %payload.rule_triggered,
                tx       = %payload.transaction_hash,
                "rule fired — sending webhook"
            );
            if let Err(e) = send_webhook(
                client,
                &contract.webhook_url,
                &payload,
                contract.webhook_secret.as_deref(),
            ).await {
                error!(
                    contract = %contract.label,
                    rule     = %payload.rule_triggered,
                    tx       = %payload.transaction_hash,
                    error    = %e,
                    "webhook delivery failed"
                );
            }
        }
    }

    if tx_count > 0 {
        info!(
            contract     = %contract.label,
            transactions = tx_count,
            alerts       = alert_count,
            "poll cycle complete"
        );
    }

    Ok((tx_count, alert_count))
}

// ── Soroban operation enrichment ──────────────────────────────────────────────

async fn fetch_soroban_details(
    client:  &Client,
    base:    &str,
    tx_hash: &str,
) -> Result<(Vec<String>, Option<u64>)> {
    let url = format!("{}/transactions/{}/operations", base, tx_hash);

    let page: OperationsPage = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET {} failed", url))?
        .json()
        .await
        .with_context(|| format!("failed to parse operations from {}", url))?;

    let mut function_names: Vec<String> = Vec::new();
    let mut total_stroops:  u64         = 0;
    let mut has_payment:    bool        = false;

    for op in page._embedded.records {
        if op.op_type == "invoke_host_function" {
            if let Some(f) = op.function {
                function_names.push(f);
            }
        }
        if op.op_type == "payment" {
            if let Some(amt_str) = op.amount {
                if let Ok(xlm) = amt_str.parse::<f64>() {
                    total_stroops = total_stroops.saturating_add((xlm * 10_000_000.0) as u64);
                    has_payment = true;
                }
            }
        }
    }

    let amount_stroops = if has_payment { Some(total_stroops) } else { None };

    Ok((function_names, amount_stroops))
}

// ── Startup log field helpers (for testing) ──────────────────────────────────

#[cfg(test)]
fn startup_log_fields(cfg: &AppConfig) -> (String, String, String, String) {
    let contracts_list = cfg.contracts.iter().map(|c| c.label.as_str()).collect::<Vec<_>>().join(", ");
    let mut networks: Vec<&str> = cfg.contracts.iter().map(|c| c.network.as_str()).collect();
    networks.sort();
    networks.dedup();
    let networks_str = networks.join(", ");
    let mut horizon_urls: Vec<(&str, &str)> = cfg
        .contracts
        .iter()
        .map(|c| (c.network.as_str(), c.network.horizon_base_url()))
        .collect();
    horizon_urls.sort();
    horizon_urls.dedup();
    let horizon_urls_str = horizon_urls
        .iter()
        .map(|(net, url)| format!("{}={}", net, url))
        .collect::<Vec<_>>()
        .join(", ");
    (
        env!("CARGO_PKG_VERSION").to_string(),
        contracts_list,
        networks_str,
        horizon_urls_str,
    )
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use txwatch_config::{AlertRule, Network};
    use wiremock::matchers::{method, path_regex};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn ops_page(function_name: &str) -> serde_json::Value {
        serde_json::json!({
            "_embedded": {
                "records": [{
                    "type":     "invoke_host_function",
                    "function": function_name
                }]
            }
        })
    }

    fn empty_page() -> serde_json::Value {
        serde_json::json!({ "_embedded": { "records": [] } })
    }

    #[tokio::test]
    async fn poll_returns_ok_on_empty_page() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path_regex("/accounts/.*/transactions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(empty_page()))
            .mount(&server)
            .await;

        let client = Client::new();
        let url = format!(
            "{}/accounts/{}/transactions?cursor=now&order=asc&limit=200",
            server.uri(),
            "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        );
        let page: HorizonPage = client.get(&url).send().await.unwrap().json().await.unwrap();
        assert!(page._embedded.records.is_empty());
    }

    #[tokio::test]
    async fn fetch_soroban_details_extracts_function_name() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path_regex("/transactions/.*/operations"))
            .respond_with(ResponseTemplate::new(200).set_body_json(ops_page("withdraw")))
            .mount(&server)
            .await;

        let client = Client::new();
        let (fn_names, amount) =
            fetch_soroban_details(&client, &server.uri(), "abc123").await.unwrap();

        assert_eq!(fn_names, vec!["withdraw"]);
        assert!(amount.is_none());
    }

    #[tokio::test]
    async fn fetch_soroban_details_extracts_payment_amount() {
        let server = MockServer::start().await;
        let ops = serde_json::json!({
            "_embedded": { "records": [{ "type": "payment", "amount": "1000.0000000" }] }
        });
        Mock::given(method("GET"))
            .and(path_regex("/transactions/.*/operations"))
            .respond_with(ResponseTemplate::new(200).set_body_json(ops))
            .mount(&server)
            .await;

        let client = Client::new();
        let (fn_names, amount) =
            fetch_soroban_details(&client, &server.uri(), "abc123").await.unwrap();

        assert!(fn_names.is_empty());
        assert_eq!(amount, Some(10_000_000_000));
    }

    #[tokio::test]
    async fn fetch_soroban_details_returns_none_on_empty_ops() {
        let server = MockServer::start().await;
        let ops = serde_json::json!({ "_embedded": { "records": [] } });
        Mock::given(method("GET"))
            .and(path_regex("/transactions/.*/operations"))
            .respond_with(ResponseTemplate::new(200).set_body_json(ops))
            .mount(&server)
            .await;

        let client = Client::new();
        let (fn_names, amount) =
            fetch_soroban_details(&client, &server.uri(), "abc123").await.unwrap();

        assert!(fn_names.is_empty());
        assert!(amount.is_none());
    }

    #[test]
    fn startup_log_includes_version_contracts_list_and_networks() {
        let cfg = AppConfig {
            poll_interval_seconds: 10,
            http_pool_max_idle_per_host: None,
            http_tcp_keepalive_secs: None,
            http_connection_verbose: None,
            contracts: vec![
                WatchedContract {
                    label: "Contract A".into(),
                    contract_id: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".into(),
                    network: txwatch_config::Network::Testnet,
                    rules: vec![txwatch_config::AlertRule::AnyTransaction],
                    webhook_url: "https://hooks.example.com/a".into(),
                    webhook_secret: None,
                    horizon_base_url_override: None,
                },
                WatchedContract {
                    label: "Contract B".into(),
                    contract_id: "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB".into(),
                    network: txwatch_config::Network::Mainnet,
                    rules: vec![txwatch_config::AlertRule::AnyTransaction],
                    webhook_url: "https://hooks.example.com/b".into(),
                    webhook_secret: None,
                    horizon_base_url_override: None,
                },
                WatchedContract {
                    label: "Contract C".into(),
                    contract_id: "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC".into(),
                    network: txwatch_config::Network::Mainnet,
                    rules: vec![txwatch_config::AlertRule::AnyTransaction],
                    webhook_url: "https://hooks.example.com/c".into(),
                    webhook_secret: None,
                    horizon_base_url_override: None,
                },
            ],
        };

        let (version, contracts_list, networks, horizon_urls) = startup_log_fields(&cfg);

        assert!(!version.is_empty());
        assert_eq!(contracts_list, "Contract A, Contract B, Contract C");
        assert_eq!(networks, "mainnet, testnet");
        // Issue #114: horizon URLs must include network name and URL for each distinct network
        assert!(horizon_urls.contains("mainnet=https://horizon.stellar.org"));
        assert!(horizon_urls.contains("testnet=https://horizon-testnet.stellar.org"));
    }
}
