Priority: High
Crate: txwatch (cli), txwatch-poller
Labels: reliability, enhancement

Description:
The watch command runs txwatch_poller::run(cfg).await? with no signal handling. A SIGINT or SIGTERM immediately terminates the process, potentially interrupting a webhook POST mid-flight.

Tasks:

Add tokio::signal::ctrl_c() handling in main
Propagate a shutdown token to the poller loop so it finishes the current poll cycle before exiting
Log a clean shutdown message
.......................................................
Priority: High
Crate: txwatch-rules
Labels: bug, security

Description:
eval_rule for LargeTransfer uses threshold_xlm.checked_mul(10_000_000) and returns an error on overflow. The config validator does not enforce an upper bound, so a misconfigured threshold silently causes the rule to always return an error and never fire, logged only as a warning.

Tasks:

Add an upper-bound validation in AlertRule::validate for LargeTransfer (e.g. max 1_000_000_000 XLM)
Add a test asserting the validator rejects an absurdly large threshold
Add a test asserting the rule returns an error (not a panic) when overflow would occur
......................................................
Priority: High
Crate: txwatch (cli)
Labels: enhancement

Description:
The validate subcommand prints contract label, ID, webhook URL, secret status, and rule count — but not the actual rules. A user cannot confirm their rule configuration is parsed correctly without reading the raw TOML.

Tasks:

Print each rule label string under the contract summary
Add a snapshot test for the validate output format
.........................................................
Priority: High
Crate: txwatch-poller
Labels: testing

Description:
The existing poller tests only test fetch_soroban_details and raw page deserialization in isolation. There is no test that exercises poll_contract end-to-end.

Tasks:

Add an integration test that mounts mock servers for both the Horizon transactions endpoint and the webhook receiver
Assert the webhook receives the correct payload for a LargeTransfer rule
Assert the cursor is advanced after the poll