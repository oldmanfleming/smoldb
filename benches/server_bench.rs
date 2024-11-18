use assert_cmd::prelude::*;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rayon::prelude::*;
use smoldb::Client;
use std::{
    process::Command,
    sync::mpsc::{self},
    thread,
    time::Duration,
};
use tempfile::TempDir;

const ADDR: &str = "127.0.0.1:4011";
const NUM_OPS: u64 = 1000;

// TODO: It seems that due to the limitations of the client being single-threaded and opening a new
// connection for each operation, we reach some OS limitations when trying to run this with a high number of ops.
// This should be fixed when we implement a connection pool in the client.

fn get_bench(c: &mut Criterion) {
    let storage_types = &["bitcask", "sled"];
    let threads = &["1", "2", "4", "8", "16", "32"];
    let thread_pools = &["naive", "rayon", "shared-queue"];

    let mut group = c.benchmark_group("get_bench");

    for &storage in storage_types {
        for &pool in thread_pools {
            for &t in threads {
                let params = (storage, pool, t);
                group.bench_with_input(
                    BenchmarkId::new("get_bench", format!("{}_{}_{}", storage, pool, t)),
                    &params,
                    |b, &(storage, pool, t)| {
                        let temp_dir = TempDir::new().unwrap();
                        let (sender, handle) = start_server(&temp_dir, storage, pool, t, ADDR);
                        let ops: Vec<u64> = (0..NUM_OPS).collect();

                        ops.par_iter().for_each(|i| {
                            let mut client = Client::connect(&ADDR.parse().unwrap()).unwrap();
                            client
                                .set(format!("key{}", i), "value".to_string())
                                .unwrap();
                        });

                        b.iter(|| {
                            ops.par_iter().for_each(|i| {
                                let mut client = Client::connect(&ADDR.parse().unwrap()).unwrap();
                                client.get(format!("key{}", i)).unwrap();
                            });
                        });

                        sender.send(()).unwrap();
                        handle.join().unwrap();
                    },
                );
            }
        }
    }
    group.finish();
}

fn set_bench(c: &mut Criterion) {
    let storage_types = &["bitcask", "sled"];
    let threads = &["1", "2", "4", "8", "16", "32"];
    let thread_pools = &["naive", "rayon", "shared-queue"];

    let mut group = c.benchmark_group("set_bench");

    for &storage in storage_types {
        for &pool in thread_pools {
            for &t in threads {
                let params = (storage, pool, t);
                group.bench_with_input(
                    BenchmarkId::new("set_bench", format!("{}_{}_{}", storage, pool, t)),
                    &params,
                    |b, &(storage, pool, t)| {
                        let temp_dir = TempDir::new().unwrap();
                        let (sender, handle) =
                            start_server(&temp_dir, storage, pool, &t.to_string(), ADDR);
                        let ops: Vec<u64> = (0..NUM_OPS).collect();

                        b.iter(|| {
                            ops.par_iter().for_each(|i| {
                                let mut client = Client::connect(&ADDR.parse().unwrap()).unwrap();
                                client
                                    .set(format!("key{}", i), "value".to_string())
                                    .unwrap();
                            });
                        });

                        sender.send(()).unwrap();
                        handle.join().unwrap();
                    },
                );
            }
        }
    }
    group.finish();
}

fn start_server(
    temp_dir: &TempDir,
    storage: &str,
    thread_pool: &str,
    pool_size: &str,
    addr: &str,
) -> (mpsc::SyncSender<()>, thread::JoinHandle<()>) {
    let (sender, receiver) = mpsc::sync_channel(0);
    let mut server = Command::cargo_bin("smoldb").unwrap();
    let mut child = server
        .args(&[
            "--storage",
            storage,
            "--pool",
            thread_pool,
            "--pool-size",
            pool_size,
            "--addr",
            addr,
        ])
        .current_dir(&temp_dir)
        .spawn()
        .unwrap();
    let handle = thread::spawn(move || {
        let _ = receiver.recv(); // wait for main thread to finish
        child.kill().expect("server exited before killed");
    });
    thread::sleep(Duration::from_secs(1));

    (sender, handle)
}

criterion_group!(benches, get_bench, set_bench);
criterion_main!(benches);
