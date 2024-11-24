# smoldb

A naive distributed sql database written in Rust

### ⚠️ **Warning**

This projet is for learning purposes only and should be evaluated accordingly

### Learning Goals

- [x] Configurable key-value storage engine with a custom
      [Bitcask](https://riak.com/assets/bitcask-intro.pdf) implementation and
      [Sled](https://crates.io/crates/sled) for comparison
- [x] Client & Server architecture with async TCP communication using
      [Tokio](https://crates.io/crates/tokio)
- [x] Client-side connection pooling
- [ ] ACID transaction support with
      [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control)
- [ ] SQL interface support
- [ ] Distributed consensus and replication using
      [Raft](https://raft.github.io/raft.pdf)
