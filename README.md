# smoldb

A naive distributed sql database written in Rust entirely for learning purposes.

### Learning Goals

- [x] Persistent key-value storage using
      [Bitcask](https://riak.com/assets/bitcask-intro.pdf)
- [ ] Concurrent network access through a multi-threaded client-server
      architecture
- [ ] Distributed transactions with ACID snapshot-isolation using
      [Percolator](https://storage.googleapis.com/pub-tools-public-publication-data/pdf/36726.pdf)
- [ ] Distributed replication using [Raft](https://raft.github.io/raft.pdf)
