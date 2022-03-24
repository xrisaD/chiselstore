# ChiselStore (with OmniPaxos)

[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

ChiselStore is an embeddable, distributed [SQLite](https://www.sqlite.org/index.html) for Rust. The original ChiselStore is powered by [Little Raft](https://github.com/andreev-io/little-raft). At this repository, we replaced Little Raft with [OmniPaxos](https://github.com/haraldng/omnipaxos), a replicated log library implemented in Rust.

## Getting Started

See the [example server](examples) of how to use the ChiselStore (with OmniPaxos) library.

