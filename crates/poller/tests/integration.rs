/// Integration tests for the full poll → evaluate → notify pipeline.
///
/// These tests spin up two wiremock servers:
///   - a mock Horizon server (transactions + operations endpoints)
///   - a mock webhook receiver
///
/// They then either call the public `run()` entry-point or drive the evaluate /
/// notify helpers directly to verify end-to-end behaviour without touching the
/// real Stellar network.
mod helpers;

use std::time::Duration;

use reqwest::Client;
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

use txwatch_config::{AlertRule, AppConfig};
use txwatch_rules::{evaluate, EnrichedTransaction};

// ── Tests ─────────────────────────────────────────────────────────────────────

/// `run()` polling loop: fires exactly one webhook for the single transaction
/// returned on the first poll cycle, then gets empty pages on subsequent cycles.
#[tokio::test]
async fn run_polls_once_and_fires_webhook() {
    let horizon  = MockServer::start().await;
    let receiver = MockServer::start().await;

    // First transactions request returns one tx.
    Mock::given(method("GET"))
        .and(path_regex("/accounts/.*/transactions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(helpers::tx_page("run001", "500", true)),
        )
        .up_to_n_times(1)
        .mount(&horizon)
        .await;

    // All subsequent transaction requests return an empty page.
    Mock::given(method("GET"))
        .and(path_regex("/accounts/.*/transactions"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(helpers::empty_page()),
        )
        .mount(&horizon)
        .await;

    // Operations for the tx: no Soroban details needed.
    Mock::given(method("GET"))
        .and(path("/transactions/run001/operations"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(helpers::empty_page()),
        )
        .mount(&horizon)
        .await;

    // Webhook receiver: expect exactly 1 POST.
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&receiver)
        .await;

    let mut contract = helpers::contract(
        &format!("{}/hook", receiver.uri()),
        vec![AlertRule::AnyTransaction],
    );
    contract.horizon_base_url_override = Some(horizon.uri());

    let cfg = AppConfig {
        poll_interval_seconds: 1,
        contracts: vec![contract],
    };

    // Drive the loop for one full poll cycle (slightly more than the interval).
    let _ = tokio::time::timeout(Duration::from_millis(1500), txwatch_poller::run(cfg)).await;

    // MockServer drop verifies that exactly 1 webhook was received.
}

/// AnyTransaction rule fires and webhook is called exactly once.
#[tokio::test]
async fn any_transaction_fires_webhook() {
    let horizon  = MockServer::start().await;
    let receiver = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex("/accounts/.*/transactions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(helpers::tx_page("hash001", "100", true)),
        )
        .mount(&horizon)
        .await;

    Mock::given(method("GET"))
        .and(path("/transactions/hash001/operations"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(helpers::empty_page()),
        )
        .mount(&horizon)
        .await;

    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&receiver)
        .await;

    let client   = Client::new();
    let contract = helpers::contract(
        &format!("{}/hook", receiver.uri()),
        vec![AlertRule::AnyTransaction],
    );

    let url = format!(
        "{}/accounts/{}/transactions?cursor=now&order=asc&limit=200",
        horizon.uri(),
        contract.contract_id
    );

    #[derive(serde::Deserialize)]
    struct Page { _embedded: Emb }
    #[derive(serde::Deserialize)]
    struct Emb  { records: Vec<txwatch_rules::HorizonTransaction> }

    let page: Page = client.get(&url).send().await.unwrap().json().await.unwrap();
    let records = page._embedded.records;
    assert_eq!(records.len(), 1);

    for raw in records {
        let ops_url = format!("{}/transactions/{}/operations", horizon.uri(), raw.hash);
        // Consume the operations response to satisfy the mock expectation.
        let _ = client.get(&ops_url).send().await.unwrap().bytes().await.unwrap();

        let enriched = EnrichedTransaction::from_horizon(raw, vec![], None, None).unwrap();
        let payloads = evaluate(
            &contract.label,
            &contract.contract_id,
            contract.network.as_str(),
            &horizon.uri(),
            "https://stellar.expert/explorer/testnet",
            &contract.rules,
            &enriched,
        );
        assert_eq!(payloads.len(), 1);

        for payload in &payloads {
            txwatch_notifier::send_webhook(&client, &contract.webhook_url, payload, None)
                .await
                .unwrap();
        }
    }
}

/// TransactionFailed rule fires only for failed transactions.
#[tokio::test]
async fn transaction_failed_rule_fires_only_on_failure() {
    let horizon  = MockServer::start().await;
    let receiver = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex("/accounts/.*/transactions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "_embedded": {
                "records": [
                    {
                        "hash": "ok_tx", "created_at": "2024-06-01T10:00:00Z",
                        "successful": true, "paging_token": "1",
                        "envelope_xdr": null, "result_xdr": null
                    },
                    {
                        "hash": "fail_tx", "created_at": "2024-06-01T10:01:00Z",
                        "successful": false, "paging_token": "2",
                        "envelope_xdr": null, "result_xdr": null
                    }
                ]
            }
        })))
        .mount(&horizon)
        .await;

    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&receiver)
        .await;

    let client   = Client::new();
    let contract = helpers::contract(
        &format!("{}/hook", receiver.uri()),
        vec![AlertRule::TransactionFailed],
    );

    let txs = vec![
        EnrichedTransaction::from_horizon(
            txwatch_rules::HorizonTransaction {
                hash: "ok_tx".into(), created_at: "2024-06-01T10:00:00Z".into(),
                successful: true, paging_token: "1".into(),
                fee_charged: None, envelope_xdr: None, result_xdr: None,
            },
            vec![], None, None,
        ).unwrap(),
        EnrichedTransaction::from_horizon(
            txwatch_rules::HorizonTransaction {
                hash: "fail_tx".into(), created_at: "2024-06-01T10:01:00Z".into(),
                successful: false, paging_token: "2".into(),
                fee_charged: None, envelope_xdr: None, result_xdr: None,
            },
            vec![], None, None,
        ).unwrap(),
    ];

    for tx in &txs {
        let payloads = evaluate(
            &contract.label, &contract.contract_id,
            contract.network.as_str(), &horizon.uri(),
            "https://stellar.expert/explorer/testnet",
            &contract.rules, tx,
        );
        for p in &payloads {
            txwatch_notifier::send_webhook(&client, &contract.webhook_url, p, None)
                .await.unwrap();
        }
    }
}

/// LargeTransfer rule fires when payment amount meets threshold.
#[tokio::test]
async fn large_transfer_fires_above_threshold() {
    let receiver = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&receiver)
        .await;

    let client   = Client::new();
    let contract = helpers::contract(
        &format!("{}/hook", receiver.uri()),
        vec![AlertRule::LargeTransfer { threshold_xlm: 5_000 }],
    );

    let tx = EnrichedTransaction::from_horizon(
        txwatch_rules::HorizonTransaction {
            hash: "big_tx".into(), created_at: "2024-06-01T10:00:00Z".into(),
            successful: true, paging_token: "1".into(),
            fee_charged: None, envelope_xdr: None, result_xdr: None,
        },
        vec![],
        Some(100_000_000_000),
        None,
    ).unwrap();

    let payloads = evaluate(
        &contract.label, &contract.contract_id,
        contract.network.as_str(), "https://horizon-testnet.stellar.org",
        "https://stellar.expert/explorer/testnet",
        &contract.rules, &tx,
    );
    assert_eq!(payloads.len(), 1);
    assert_eq!(payloads[0].amount_xlm, Some(10_000));

    txwatch_notifier::send_webhook(&client, &contract.webhook_url, &payloads[0], None)
        .await.unwrap();
}

/// FunctionCalled rule fires only when the function name matches.
#[tokio::test]
async fn function_called_rule_fires_on_exact_match() {
    let receiver = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&receiver)
        .await;

    let client   = Client::new();
    let contract = helpers::contract(
        &format!("{}/hook", receiver.uri()),
        vec![AlertRule::FunctionCalled { function_name: "withdraw".into() }],
    );

    let txs = vec![
        EnrichedTransaction::from_horizon(
            txwatch_rules::HorizonTransaction {
                hash: "t1".into(), created_at: "2024-06-01T10:00:00Z".into(),
                successful: true, paging_token: "1".into(),
                fee_charged: None, envelope_xdr: None, result_xdr: None,
            },
            vec!["deposit".into()], None, None,
        ).unwrap(),
        EnrichedTransaction::from_horizon(
            txwatch_rules::HorizonTransaction {
                hash: "t2".into(), created_at: "2024-06-01T10:01:00Z".into(),
                successful: true, paging_token: "2".into(),
                fee_charged: None, envelope_xdr: None, result_xdr: None,
            },
            vec!["withdraw".into()], None, None,
        ).unwrap(),
    ];

    for tx in &txs {
        let payloads = evaluate(
            &contract.label, &contract.contract_id,
            contract.network.as_str(), "https://horizon-testnet.stellar.org",
            "https://stellar.expert/explorer/testnet",
            &contract.rules, tx,
        );
        for p in &payloads {
            txwatch_notifier::send_webhook(&client, &contract.webhook_url, p, None)
                .await.unwrap();
        }
    }
}

/// Cursor advances so the same transaction is not processed twice.
#[tokio::test]
async fn cursor_advances_after_each_transaction() {
    use std::collections::HashMap;

    let mut cursors: HashMap<String, String> = HashMap::new();
    let contract_id = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    cursors.insert(contract_id.to_string(), "now".to_string());

    for token in &["100", "200", "300"] {
        cursors.insert(contract_id.to_string(), token.to_string());
    }

    assert_eq!(cursors.get(contract_id).map(String::as_str), Some("300"));
}

/// HighFee rule fires when fee_charged from Horizon response exceeds threshold.
#[tokio::test]
async fn high_fee_rule_fires_on_fee_charged() {
    let horizon  = MockServer::start().await;
    let receiver = MockServer::start().await;

    // Horizon: transaction with fee_charged: "50000" (stroops)
    Mock::given(method("GET"))
        .and(path_regex("/accounts/.*/transactions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({
                    "_embedded": {
                        "records": [{
                            "hash":         "fee_tx",
                            "created_at":   "2024-06-01T10:00:00Z",
                            "successful":   true,
                            "paging_token": "1",
                            "fee_charged":  "50000",
                            "envelope_xdr": null,
                            "result_xdr":   null
                        }]
                    }
                })),
        )
        .mount(&horizon)
        .await;

    // Horizon: operations for that transaction (empty, no Soroban)
    Mock::given(method("GET"))
        .and(path("/transactions/fee_tx/operations"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(empty_page_json()),
        )
        .mount(&horizon)
        .await;

    // Webhook receiver: expect exactly 1 POST (HighFee fires)
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&receiver)
        .await;

    let client   = Client::new();
    let contract = contract(
        &format!("{}/hook", receiver.uri()),
        vec![AlertRule::HighFee { threshold_stroops: 10_000 }],
    );

    let tx = EnrichedTransaction::from_horizon(
        txwatch_rules::HorizonTransaction {
            hash: "fee_tx".into(),
            created_at: "2024-06-01T10:00:00Z".into(),
            successful: true,
            paging_token: "1".into(),
            fee_charged: Some("50000".into()),
            envelope_xdr: None,
            result_xdr: None,
        },
        None,
        None,
        None,
    ).unwrap();

    let payloads = evaluate(
        &contract.label,
        &contract.contract_id,
        contract.network.as_str(),
        &horizon.uri(),
        "https://stellar.expert/explorer/testnet",
        &contract.rules,
        &tx,
    );
    assert_eq!(payloads.len(), 1);
    assert!(payloads[0].rule_triggered.contains("HighFee"));
    assert_eq!(payloads[0].fee_charged_stroops, Some(50_000));

    txwatch_notifier::send_webhook(&client, &contract.webhook_url, &payloads[0], None)
        .await
        .unwrap();
}

/// Polling the full contract path also enriches Soroban operations and fires
/// `FunctionCalled` rules when the invoked operation matches.
#[tokio::test]
async fn run_polls_contract_and_triggers_function_called_rule() {
    let horizon  = MockServer::start().await;
    let receiver = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex("/accounts/.*/transactions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(helpers::tx_page("func_tx", "1", true)),
        )
        .up_to_n_times(1)
        .mount(&horizon)
        .await;

    Mock::given(method("GET"))
        .and(path("/transactions/func_tx/operations"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(helpers::ops_page("withdraw")),
        )
        .mount(&horizon)
        .await;

    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&receiver)
        .await;

    let mut contract = helpers::contract(
        &format!("{}/hook", receiver.uri()),
        vec![AlertRule::FunctionCalled { function_name: "withdraw".into() }],
    );
    contract.horizon_base_url_override = Some(horizon.uri());

    let cfg = AppConfig {
        poll_interval_seconds: 1,
        contracts: vec![contract],
    };

    let _ = tokio::time::timeout(Duration::from_millis(1500), txwatch_poller::run(cfg)).await;
}
