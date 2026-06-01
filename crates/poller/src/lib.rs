use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;
use tracing::{error, info, warn};
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
    transactions: AtomicU64,
    alerts:       AtomicU64,
}

// ── Public entry point ────────────────────────────────────────────────────────

/// Run the polling loop forever. Each contract is polled sequentially;
/// a single bad transaction never stops the loop.
/// Logs a summary every 60 seconds: contracts watched, transactions processed,
/// alerts fired.
pub async fn run(cfg: AppConfig) -> Result<()> {
    let client = Client::builder()
        .timeout(Duration::from_secs(15))
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

    info!(
        contracts     = n_contracts,
        interval_secs = cfg.poll_interval_seconds,
        "TxWatch polling engine started"
    );

    // Spawn the summary logger task.
    let counters_clone = Arc::clone(&counters);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(summary_every).await;
            info!(
                contracts    = n_contracts,
                transactions = counters_clone.transactions.load(Ordering::Relaxed),
                alerts       = counters_clone.alerts.load(Ordering::Relaxed),
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
    let cursor = cursors
        .get(&contract.contract_id)
        .cloned()
        .unwrap_or_else(|| "now".to_string());

    let base = contract.network.horizon_base_url();
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

    let mut tx_count    = 0u64;
    let mut alert_count = 0u64;

    for raw_tx in records {
        let paging_token = raw_tx.paging_token.clone();
        let tx_hash      = raw_tx.hash.clone();

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

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path_regex};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[allow(dead_code)]
    fn tx_page(hash: &str, paging_token: &str, successful: bool) -> serde_json::Value {
        serde_json::json!({
            "_embedded": {
                "records": [{
                    "hash":         hash,
                    "created_at":   "2024-01-15T12:00:00Z",
                    "successful":   successful,
                    "paging_token": paging_token,
                    "fee_charged":  "100",
                    "envelope_xdr": null,
                    "result_xdr":   null
                }]
            }
        })
    }

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

    // ── Issue #78: sum multiple payment operations ────────────────────────────

    #[tokio::test]
    async fn fetch_soroban_details_sums_multiple_payment_amounts() {
        let server = MockServer::start().await;
        // Two payment ops of 5000 XLM each → should sum to 10_000 XLM = 100_000_000_000 stroops
        let ops = serde_json::json!({
            "_embedded": {
                "records": [
                    { "type": "payment", "amount": "5000.0000000" },
                    { "type": "payment", "amount": "5000.0000000" }
                ]
            }
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
        // 5000 + 5000 = 10_000 XLM = 100_000_000_000 stroops
        assert_eq!(amount, Some(100_000_000_000));
    }

    #[tokio::test]
    async fn fetch_soroban_details_sums_three_payment_ops() {
        let server = MockServer::start().await;
        let ops = serde_json::json!({
            "_embedded": {
                "records": [
                    { "type": "payment", "amount": "100.0000000" },
                    { "type": "payment", "amount": "200.0000000" },
                    { "type": "payment", "amount": "300.0000000" }
                ]
            }
        });
        Mock::given(method("GET"))
            .and(path_regex("/transactions/.*/operations"))
            .respond_with(ResponseTemplate::new(200).set_body_json(ops))
            .mount(&server)
            .await;

        let client = Client::new();
        let (_fn_names, amount) =
            fetch_soroban_details(&client, &server.uri(), "abc123").await.unwrap();

        // 100 + 200 + 300 = 600 XLM = 6_000_000_000 stroops
        assert_eq!(amount, Some(6_000_000_000));
    }

    // ── Issue #77: multiple invoke_host_function operations ───────────────────

    #[tokio::test]
    async fn fetch_soroban_details_captures_all_function_names() {
        let server = MockServer::start().await;
        // Transaction with two Soroban invocations
        let ops = serde_json::json!({
            "_embedded": {
                "records": [
                    { "type": "invoke_host_function", "function": "deposit" },
                    { "type": "invoke_host_function", "function": "withdraw" }
                ]
            }
        });
        Mock::given(method("GET"))
            .and(path_regex("/transactions/.*/operations"))
            .respond_with(ResponseTemplate::new(200).set_body_json(ops))
            .mount(&server)
            .await;

        let client = Client::new();
        let (fn_names, amount) =
            fetch_soroban_details(&client, &server.uri(), "abc123").await.unwrap();

        // Both function names must be captured, in order
        assert_eq!(fn_names, vec!["deposit", "withdraw"]);
        assert!(amount.is_none());
    }

    #[tokio::test]
    async fn fetch_soroban_details_captures_mixed_ops() {
        let server = MockServer::start().await;
        // One Soroban invocation + one payment in the same transaction
        let ops = serde_json::json!({
            "_embedded": {
                "records": [
                    { "type": "invoke_host_function", "function": "transfer" },
                    { "type": "payment", "amount": "1000.0000000" }
                ]
            }
        });
        Mock::given(method("GET"))
            .and(path_regex("/transactions/.*/operations"))
            .respond_with(ResponseTemplate::new(200).set_body_json(ops))
            .mount(&server)
            .await;

        let client = Client::new();
        let (fn_names, amount) =
            fetch_soroban_details(&client, &server.uri(), "abc123").await.unwrap();

        assert_eq!(fn_names, vec!["transfer"]);
        assert_eq!(amount, Some(10_000_000_000));
    }
}
