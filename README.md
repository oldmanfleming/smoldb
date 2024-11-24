# smoldb

A naive distributed sql database written in Rust

### ⚠️ **Warning**

This projet is for learning purposes only and should be evaluated accordingly

### Learning Goals

- [x] Custom [Bitcask](https://riak.com/assets/bitcask-intro.pdf) key-value
      storage engine
- [x] Client & Server architecture with async tcp networking
- [x] Client-side connection pooling
- [ ] ACID transaction support with
      [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control)
- [ ] SQL interface support
- [ ] Distributed consensus and replication using
      [Raft](https://raft.github.io/raft.pdf)
