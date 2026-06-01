#![forbid(unsafe_code)]

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use txwatch_config::AlertRule;

// ── Constants ──────────────────────────────────────────────────────────────────

/// Maximum XLM supply in stroops: 50 billion XLM × 10^7 stroops/XLM.
/// The total Stellar XLM supply is capped at ~50 billion XLM. This constant serves
/// as a reference for validating that u64 is sufficient for any realistic transaction
/// amount, since 500 trillion is well below u64::MAX (18.4 quintillion).
pub const MAX_XLM_SUPPLY_STROOPS: u64 = 500_000_000_000_000_000;

// ── Horizon transaction shape ─────────────────────────────────────────────────

/// Raw Horizon transaction record as returned by the REST API.
#[derive(Debug, Clone, Deserialize)]
pub struct HorizonTransaction {
    pub hash: String,
    pub created_at: String, // RFC 3339
    pub successful: bool,
    pub paging_token: String,
    /// Fee charged in stroops (Horizon returns this as a string).
    pub fee_charged: Option<String>,
    /// Base64-encoded XDR transaction envelope.
    pub envelope_xdr: Option<String>,
    /// Base64-encoded XDR transaction result.
    pub result_xdr: Option<String>,
}

// ── Enriched transaction ──────────────────────────────────────────────────────

/// A transaction enriched with Soroban-specific fields extracted from the
/// Horizon `operations` sub-resource JSON (returned inline via `join=operations`
/// or fetched separately). We keep this as a plain struct so rule evaluation
/// stays pure and testable without network calls.
#[derive(Debug, Clone)]
pub struct EnrichedTransaction {
    pub hash: String,
    pub timestamp: DateTime<Utc>,
    pub successful: bool,
    pub paging_token: String,
    /// All Soroban contract functions invoked in this transaction (may be multiple).
    pub function_names: Vec<String>,
    /// Transfer amount in stroops (1 XLM = 10_000_000 stroops), if detected.
    /// Uses u64 because the total XLM supply is ~50 billion XLM = ~500 trillion stroops,
    /// which is well within u64::MAX (18.4 quintillion). This type is sufficient for any
    /// realistic transaction amount on the Stellar network.
    pub amount_stroops: Option<u64>,
    /// Fee charged for this transaction in stroops.
    pub fee_charged_stroops: Option<u64>,
}

impl EnrichedTransaction {
    /// Build from a raw Horizon record plus optional Soroban operation details.
    pub fn from_horizon(
        tx: HorizonTransaction,
        function_names: Vec<String>,
        amount_stroops: Option<u64>,
        fee_charged_stroops: Option<u64>,
    ) -> Result<Self> {
        let timestamp = tx.created_at.parse::<DateTime<Utc>>().with_context(|| {
            format!(
                "cannot parse timestamp '{}' for tx {}",
                tx.created_at, tx.hash
            )
        })?;

        Ok(Self {
            hash:          tx.hash,
            timestamp,
            successful:    tx.successful,
            paging_token:  tx.paging_token,
            function_names,
            amount_stroops,
            fee_charged_stroops: fee_charged_stroops.or_else(|| {
                tx.fee_charged
                    .as_deref()
                    .and_then(|s| s.parse::<u64>().ok())
            }),
        })
    }
}

// ── AlertPayload ──────────────────────────────────────────────────────────────

/// The JSON body POSTed to the webhook URL when a rule fires.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AlertPayload {
    pub label: String,
    pub contract_id: String,
    pub network: String,
    /// Stable machine-readable rule variant (e.g. `"LargeTransfer"`).
    pub rule_type: String,
    pub rule_triggered: String,
    pub transaction_hash: String,
    /// First invoked function name (backward-compat singular field).
    pub function_name: Option<String>,
    /// All invoked function names in this transaction.
    pub function_names: Vec<String>,
    /// Amount in whole XLM (stroops / 10_000_000), present for LargeTransfer.
    #[serde(rename = "amount_xlm")]
    pub amount_xlm: Option<u64>,
    /// Fee charged in stroops.
    pub fee_charged_stroops: Option<u64>,
    /// Unix timestamp (seconds).
    pub timestamp: i64,
    /// ISO 8601 timestamp string.
    pub timestamp_iso: String,
    pub horizon_link: String,
    /// Stellar Expert explorer link for the transaction.
    pub explorer_link: String,
}

// ── Rule evaluation ───────────────────────────────────────────────────────────

/// Evaluate all rules for one contract against one transaction.
/// Returns one `AlertPayload` per matching rule.
/// Never panics — errors in individual rule evaluation are logged and skipped.
pub fn evaluate(
    label: &str,
    contract_id: &str,
    network: &str,
    horizon_base: &str,
    explorer_base: &str,
    rules: &[AlertRule],
    tx: &EnrichedTransaction,
) -> Vec<AlertPayload> {
    let horizon_link  = format!("{}/transactions/{}", horizon_base, tx.hash);
    let explorer_link = format!("{}/tx/{}", explorer_base, tx.hash);
    let timestamp = tx.timestamp.timestamp();
    let timestamp_iso = tx.timestamp.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    rules
        .iter()
        .filter_map(|rule| {
            match eval_rule(rule, tx) {
                Ok(true) => Some(AlertPayload {
                    label:            label.to_string(),
                    contract_id:      contract_id.to_string(),
                    network:          network.to_string(),
                    rule_type:        rule_type(rule),
                    rule_triggered:   rule_label(rule),
                    transaction_hash: tx.hash.clone(),
                    function_name:    tx.function_names.first().cloned(),
                    function_names:   tx.function_names.clone(),
                    amount_xlm:       tx.amount_stroops.map(|s| s / 10_000_000),
                    fee_charged_stroops: tx.fee_charged_stroops,
                    timestamp,
                    timestamp_iso:    timestamp_iso.clone(),
                    horizon_link:     horizon_link.clone(),
                    explorer_link:    explorer_link.clone(),
                }),
                Ok(false) => None,
                Err(e) => {
                    tracing::warn!(
                        tx = %tx.hash,
                        rule = %rule_label(rule),
                        error = %e,
                        "rule evaluation error — skipping"
                    );
                    None
                }
            }
        })
        .collect()
}

// NOTE: When adding a new AlertRule variant, update both `eval_rule()` and
// `rule_label()` together. Rust's exhaustive matching catches missing arms,
// but this convention should be preserved for new rule variants.
fn eval_rule(rule: &AlertRule, tx: &EnrichedTransaction) -> Result<bool> {
    Ok(match rule {
        AlertRule::AnyTransaction => true,

        AlertRule::TransactionFailed => !tx.successful,

        AlertRule::LargeTransfer { threshold_xlm } => {
            let threshold_stroops = threshold_xlm
                .checked_mul(10_000_000)
                .context("threshold_xlm overflow when converting to stroops")?;
            tx.amount_stroops
                .map(|s| s >= threshold_stroops)
                .unwrap_or(false)
        }

        AlertRule::FunctionCalled { function_name } => tx
            .function_names
            .iter()
            .any(|f| f == function_name.as_str()),

        AlertRule::AdminFunctionCalled { function_names } => tx
            .function_names
            .iter()
            .any(|f| {
                let f_lower = f.to_lowercase();
                function_names.iter().any(|n| n.to_lowercase() == f_lower)
            }),

        AlertRule::HighFee { threshold_stroops, .. } => tx
            .fee_charged_stroops
            .map(|f| f >= *threshold_stroops)
            .unwrap_or(false),
    })
}

// NOTE: When adding a new AlertRule variant, update both `eval_rule()` and
// `rule_label()` together. Rust's exhaustive matching catches missing arms,
// but this convention should be preserved for new rule variants.
fn rule_label(rule: &AlertRule) -> String {
    match rule {
        AlertRule::AnyTransaction => "AnyTransaction".into(),
        AlertRule::TransactionFailed => "TransactionFailed".into(),
        AlertRule::LargeTransfer { threshold_xlm } => {
            format!("LargeTransfer(>={}XLM)", threshold_xlm)
        }
        AlertRule::FunctionCalled { function_name } => format!("FunctionCalled({})", function_name),
        AlertRule::AdminFunctionCalled { function_names } => {
            format!("AdminFunctionCalled([{}])", function_names.join(", "))
        }
        AlertRule::HighFee { threshold_stroops, threshold_xlm } => {
            if let Some(xlm) = threshold_xlm {
                format!("HighFee(>={} XLM)", xlm)
            } else {
                format!("HighFee(>={} stroops)", threshold_stroops)
            }
        }
    }
}

fn rule_type(rule: &AlertRule) -> String {
    match rule {
        AlertRule::AnyTransaction          => "AnyTransaction".into(),
        AlertRule::TransactionFailed       => "TransactionFailed".into(),
        AlertRule::LargeTransfer { .. }   => "LargeTransfer".into(),
        AlertRule::FunctionCalled { .. }  => "FunctionCalled".into(),
        AlertRule::AdminFunctionCalled { .. } => "AdminFunctionCalled".into(),
        AlertRule::HighFee { .. }         => "HighFee".into(),
    }
}

impl AlertPayload {
    /// Builder helper to override the label (used by test-webhook).
    pub fn with_label(mut self, label: String) -> Self {
        self.label = label;
        self
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;
    use txwatch_config::AlertRule;

    fn make_tx(
        successful: bool,
        function_names: &[&str],
        amount_stroops: Option<u64>,
    ) -> EnrichedTransaction {
        EnrichedTransaction {
            hash:           "abc123".into(),
            timestamp:      "2024-01-15T12:00:00Z".parse().unwrap(),
            successful,
            paging_token: "100".into(),
            function_names: function_names.iter().map(|s| s.to_string()).collect(),
            amount_stroops,
            fee_charged_stroops: None,
        }
    }

    fn run(rules: &[AlertRule], tx: &EnrichedTransaction) -> Vec<AlertPayload> {
        evaluate(
            "Label",
            "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "testnet",
            "https://horizon-testnet.stellar.org",
            "https://stellar.expert/explorer/testnet",
            rules,
            tx,
        )
    }

    #[test]
    fn any_transaction_always_fires() {
        let tx = make_tx(true, &[], None);
        let payloads = run(&[AlertRule::AnyTransaction], &tx);
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].rule_triggered, "AnyTransaction");
    }

    #[test]
    fn any_transaction_fires_on_failed_transaction() {
        let tx = make_tx(false, &[], None);
        let payloads = run(&[AlertRule::AnyTransaction], &tx);
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].rule_triggered, "AnyTransaction");
    }

    #[test]
    fn rule_label_formats_are_stable() {
        assert_eq!(rule_label(&AlertRule::AnyTransaction), "AnyTransaction");
        assert_eq!(rule_label(&AlertRule::TransactionFailed), "TransactionFailed");
        assert_eq!(rule_label(&AlertRule::LargeTransfer { threshold_xlm: 10_000 }), "LargeTransfer(>=10000XLM)");
        assert_eq!(rule_label(&AlertRule::FunctionCalled { function_name: "withdraw".into() }), "FunctionCalled(withdraw)");
        assert_eq!(rule_label(&AlertRule::AdminFunctionCalled { function_names: vec!["set_admin".into(), "upgrade".into()] }), "AdminFunctionCalled([set_admin, upgrade])");
        assert_eq!(rule_label(&AlertRule::HighFee { threshold_stroops: 10_000 }), "HighFee(>=10000 stroops)");
    }

    #[test]
    fn transaction_failed_fires_on_failure() {
        let tx = make_tx(false, &[], None);
        let payloads = run(&[AlertRule::TransactionFailed], &tx);
        assert_eq!(payloads.len(), 1);
    }

    #[test]
    fn transaction_failed_does_not_fire_on_success() {
        let tx = make_tx(true, &[], None);
        let payloads = run(&[AlertRule::TransactionFailed], &tx);
        assert!(payloads.is_empty());
    }

    #[test]
    fn large_transfer_fires_at_threshold() {
        // exactly 10_000 XLM = 100_000_000_000 stroops
        let tx = make_tx(true, &[], Some(100_000_000_000));
        let payloads = run(
            &[AlertRule::LargeTransfer {
                threshold_xlm: 10_000,
            }],
            &tx,
        );
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].amount_xlm, Some(10_000));
    }

    #[test]
    fn large_transfer_does_not_fire_below_threshold() {
        let tx = make_tx(true, &[], Some(9_999 * 10_000_000));
        let payloads = run(
            &[AlertRule::LargeTransfer {
                threshold_xlm: 10_000,
            }],
            &tx,
        );
        assert!(payloads.is_empty());
    }

    #[test]
    fn large_transfer_no_amount_does_not_fire() {
        let tx = make_tx(true, &[], None);
        let payloads = run(&[AlertRule::LargeTransfer { threshold_xlm: 1 }], &tx);
        assert!(payloads.is_empty());
    }

    #[test]
    fn large_transfer_fires_at_exact_threshold() {
        let tx = make_tx(true, &[], Some(10_000 * 10_000_000));
        let payloads = run(&[AlertRule::LargeTransfer { threshold_xlm: 10_000 }], &tx);
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].amount_xlm, Some(10_000));
    }

    #[test]
    fn large_transfer_does_not_fire_one_stroop_below_threshold() {
        let tx = make_tx(true, &[], Some(10_000 * 10_000_000 - 1));
        let payloads = run(&[AlertRule::LargeTransfer { threshold_xlm: 10_000 }], &tx);
        assert!(payloads.is_empty());
    }

    #[test]
    fn function_called_fires_on_match() {
        let tx = make_tx(true, &["withdraw"], None);
        let payloads = run(
            &[AlertRule::FunctionCalled {
                function_name: "withdraw".into(),
            }],
            &tx,
        );
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].function_name.as_deref(), Some("withdraw"));
    }

    #[test]
    fn function_called_does_not_fire_on_mismatch() {
        let tx = make_tx(true, &["deposit"], None);
        let payloads = run(
            &[AlertRule::FunctionCalled {
                function_name: "withdraw".into(),
            }],
            &tx,
        );
        assert!(payloads.is_empty());
    }

    #[test]
    fn admin_function_called_fires_on_any_match() {
        let tx = make_tx(true, &["upgrade"], None);
        let payloads = run(
            &[AlertRule::AdminFunctionCalled {
                function_names: vec!["set_admin".into(), "upgrade".into()],
            }],
            &tx,
        );
        assert_eq!(payloads.len(), 1);
        assert!(payloads[0].rule_triggered.contains("upgrade"));
    }

    #[test]
    fn function_called_does_not_fire_when_function_name_is_none() {
        let tx = make_tx(true, &[], None);
        let payloads = run(
            &[AlertRule::FunctionCalled { function_name: "withdraw".into() }],
            &tx,
        );
        assert!(payloads.is_empty());
    }

    #[test]
    fn admin_function_called_does_not_fire_when_function_name_is_none() {
        let tx = make_tx(true, &[], None);
        let payloads = run(
            &[AlertRule::AdminFunctionCalled {
                function_names: vec!["set_admin".into(), "upgrade".into()],
            }],
            &tx,
        );
        assert!(payloads.is_empty());
    }

    #[test]
    fn multiple_rules_can_fire_on_same_tx() {
        let tx = make_tx(false, &["set_admin"], Some(200_000_000_000));
        let rules = vec![
            AlertRule::AnyTransaction,
            AlertRule::TransactionFailed,
            AlertRule::LargeTransfer {
                threshold_xlm: 10_000,
            },
            AlertRule::AdminFunctionCalled {
                function_names: vec!["set_admin".into()],
            },
        ];
        let payloads = run(&rules, &tx);
        assert_eq!(payloads.len(), 4);
    }

    #[test]
    fn horizon_link_is_correct() {
        let tx = make_tx(true, &[], None);
        let payloads = run(&[AlertRule::AnyTransaction], &tx);
        assert_eq!(
            payloads[0].horizon_link,
            "https://horizon-testnet.stellar.org/transactions/abc123"
        );
    }

    #[test]
    fn url_fields_have_no_trailing_slash_and_exact_format() {
        // Verify both link fields are normalised even when base URLs have trailing slashes.
        fn run_with_bases(horizon_base: &str, explorer_base: &str) -> AlertPayload {
            let tx = EnrichedTransaction {
                hash: "deadbeef".into(),
                timestamp: "2024-01-15T12:00:00Z".parse().unwrap(),
                successful: true,
                paging_token: "1".into(),
                function_names: vec![],
                amount_stroops: None,
                fee_charged_stroops: None,
            };
            let mut payloads = evaluate(
                "L", "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "testnet", horizon_base, explorer_base,
                &[AlertRule::AnyTransaction], &tx,
            );
            payloads.remove(0)
        }

        // Without trailing slash — baseline
        let p = run_with_bases(
            "https://horizon-testnet.stellar.org",
            "https://stellar.expert/explorer/testnet",
        );
        assert_eq!(p.horizon_link,  "https://horizon-testnet.stellar.org/transactions/deadbeef");
        assert_eq!(p.explorer_link, "https://stellar.expert/explorer/testnet/tx/deadbeef");

        // With trailing slash — must produce identical output
        let p2 = run_with_bases(
            "https://horizon-testnet.stellar.org/",
            "https://stellar.expert/explorer/testnet/",
        );
        assert_eq!(p.horizon_link,  p2.horizon_link);
        assert_eq!(p.explorer_link, p2.explorer_link);
    }

    #[test]
    fn high_fee_fires_at_threshold() {
        let mut tx = make_tx(true, &[], None);
        tx.fee_charged_stroops = Some(10_000);
        let payloads = run(
            &[AlertRule::HighFee {
                threshold_stroops: 10_000,
                threshold_xlm:     None,
            }],
            &tx,
        );
        assert_eq!(payloads.len(), 1);
        assert!(payloads[0].rule_triggered.contains("HighFee"));
    }

    #[test]
    fn high_fee_does_not_fire_below_threshold() {
        let mut tx = make_tx(true, &[], None);
        tx.fee_charged_stroops = Some(9_999);
        let payloads = run(
            &[AlertRule::HighFee {
                threshold_stroops: 10_000,
                threshold_xlm:     None,
            }],
            &tx,
        );
        assert!(payloads.is_empty());
    }

    #[test]
    fn high_fee_no_fee_does_not_fire() {
        let tx = make_tx(true, &[], None);
        let payloads = run(
            &[AlertRule::HighFee {
                threshold_stroops: 1,
                threshold_xlm:     None,
            }],
            &tx,
        );
        assert!(payloads.is_empty());
    }

    #[test]
    fn enriched_transaction_parses_timestamp() {
        let raw = HorizonTransaction {
            hash: "h1".into(),
            created_at: "2024-06-01T00:00:00Z".into(),
            successful: true,
            paging_token: "1".into(),
            fee_charged: Some("100".into()),
            envelope_xdr: None,
            result_xdr: None,
        };
        let enriched = EnrichedTransaction::from_horizon(raw, vec![], None, None).unwrap();
        assert_eq!(enriched.timestamp.year(), 2024);
    }

    // ── Issue #77: multiple invoke_host_function ops ──────────────────────────

    #[test]
    fn function_called_fires_when_matching_name_is_second_in_list() {
        // Transaction has two Soroban invocations; rule should match the second
        let tx = make_tx(true, &["deposit", "withdraw"], None);
        let payloads = run(
            &[AlertRule::FunctionCalled { function_name: "withdraw".into() }],
            &tx,
        );
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].function_names, vec!["deposit", "withdraw"]);
    }

    #[test]
    fn function_called_does_not_fire_when_no_names_match() {
        let tx = make_tx(true, &["deposit", "transfer"], None);
        let payloads = run(
            &[AlertRule::FunctionCalled { function_name: "withdraw".into() }],
            &tx,
        );
        assert!(payloads.is_empty());
    }

    #[test]
    fn admin_function_called_fires_on_any_of_multiple_invocations() {
        // Two invocations; only the second is an admin function
        let tx = make_tx(true, &["transfer", "set_admin"], None);
        let payloads = run(
            &[AlertRule::AdminFunctionCalled {
                function_names: vec!["set_admin".into(), "upgrade".into()],
            }],
            &tx,
        );
        assert_eq!(payloads.len(), 1);
    }

    #[test]
    fn payload_function_names_contains_all_invocations() {
        let tx = make_tx(true, &["foo", "bar", "baz"], None);
        let payloads = run(&[AlertRule::AnyTransaction], &tx);
        assert_eq!(payloads[0].function_names, vec!["foo", "bar", "baz"]);
        // function_name (singular) is the first for backward compat
        assert_eq!(payloads[0].function_name.as_deref(), Some("foo"));
    }
}
