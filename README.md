# ChiselStore (with OmniPaxos)

[![Rust](https://github.com/chiselstrike/chiselstore/actions/workflows/rust.yml/badge.svg)](https://github.com/chiselstrike/chiselstore/actions/workflows/rust.yml)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

ChiselStore is an embeddable, distributed [SQLite](https://www.sqlite.org/index.html) for Rust. The original ChiselStore is powered by [Little Raft](https://github.com/andreev-io/little-raft). At this repository we replaced Little Raft with [OmniPaxos](https://github.com/haraldng/omnipaxos), a replicated log library implemented in Rust.

SQLite is a fast and compact relational database management system, but it is limited to single-node configurations.
With ChiselStore, you get the benefits of easy-to-use, embeddable SQLite but with Raft's high availability and fault tolerance.

## Getting Started

See the [example server](examples) of how to use the ChiselStore library.

