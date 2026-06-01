# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2025-01-01

### Added

- Real-time Soroban smart contract monitoring and webhook alert engine
- Six alert rule types: `AnyTransaction`, `TransactionFailed`, `LargeTransfer`, `FunctionCalled`, `AdminFunctionCalled`, `HighFee`
- Horizon REST API integration with cursor-based pagination for efficient polling
- TOML-based configuration with contract, rule, and webhook setup
- Support for multiple Stellar networks: mainnet, testnet, futurenet
- Webhook notification delivery with exponential backoff retry logic (up to 3 attempts)
- Webhook secret signing via `X-TxWatch-Secret` header
- Structured JSON alert payloads with transaction details and Horizon links
- Transaction enrichment with operation-level details (function names, transfer amounts)
- Fee extraction and analysis for cost monitoring
- Configurable polling intervals
- CLI commands: `watch`, `validate`, `test-webhook`
- Comprehensive configuration reference and alert rules documentation
- Integration test suite using wiremock for HTTP mocking

---

[Keep a Changelog]: https://keepachangelog.com/en/1.0.0/
