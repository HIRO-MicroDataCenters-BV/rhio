# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Improved NATS message replication logic and configuration
    - [#86](https://github.com/HIRO-MicroDataCenters-BV/rhio/pull/86)
    - [#82](https://github.com/HIRO-MicroDataCenters-BV/rhio/pull/82)
    - [#75](https://github.com/HIRO-MicroDataCenters-BV/rhio/pull/75)
- Improved S3 objects replication logic and configuration
    - [#92](https://github.com/HIRO-MicroDataCenters-BV/rhio/pull/92)
    - [#79](https://github.com/HIRO-MicroDataCenters-BV/rhio/pull/79)
    - [#74](https://github.com/HIRO-MicroDataCenters-BV/rhio/pull/74)
- Direct p2p sync between S3 buckets with bao-encoding ([#73](https://github.com/HIRO-MicroDataCenters-BV/rhio/pull/73))
- Resolve FQDN endpoints in config ([#96](https://github.com/HIRO-MicroDataCenters-BV/rhio/pull/96))
- HTTP `/health` endpoint ([#90](https://github.com/HIRO-MicroDataCenters-BV/rhio/pull/90))

## Changed

- De-duplicate topic id's for gossip overlays ([#86](https://github.com/HIRO-MicroDataCenters-BV/rhio/pull/86))

## Fixed

- Fix NATS consumers after message sync changes ([#89](https://github.com/HIRO-MicroDataCenters-BV/rhio/pull/89))
- Fix remote bucket logic after S3 sync changes ([#94](https://github.com/HIRO-MicroDataCenters-BV/rhio/pull/94))

## [0.1.0] - 2024-08-30

### Added

- GitHub CI for Rust tests and linters ([#65](https://github.com/HIRO-MicroDataCenters-BV/rhio/pull/65))
- NATS JetStream integration ([#58](https://github.com/HIRO-MicroDataCenters-BV/rhio/pull/58))
- Introduce `rhio-core` and `rhio-client` crates for client development ([#57](https://github.com/HIRO-MicroDataCenters-BV/rhio/pull/57))
- Configuration interface ([#53](https://github.com/HIRO-MicroDataCenters-BV/rhio/pull/53))
- Support for MinIO storage ([#47](https://github.com/HIRO-MicroDataCenters-BV/rhio/pull/47))
- Experimental FFI bindings for Python ([#37](https://github.com/HIRO-MicroDataCenters-BV/rhio/pull/37))

[unreleased]: https://github.com/HIRO-MicroDataCenters-BV/rhio/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/HIRO-MicroDataCenters-BV/rhio/releases/tag/v0.1.0
