// These tests are a bit clunky because bench_with_input does not support async functions.

use assert_cmd::prelude::*;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use smoldb::Client;
use std::net::SocketAddr;
use std::path::Path;
use std::process::Command;
use std::sync::mpsc::{self, SyncSender};
use std::thread::JoinHandle;
use std::{thread, time::Duration};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tokio::task::{self};

const ADDR: &str = "127.0.0.1:4011";
const NUM_OPS: u64 = 1000;

fn get_bench(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let storage_types = &["bitcask", "sled"];

    let mut group = c.benchmark_group("get_bench");

    for &storage in storage_types {
        group.bench_with_input(
            BenchmarkId::new("get_bench", format!("{:?}", storage)),
            &storage,
            |b, &storage| {
                // Setup
                let dir = TempDir::new().unwrap();
                let addr: SocketAddr = ADDR.parse().unwrap();
                let ops: Vec<u64> = (0..NUM_OPS).collect();
                let (tx, handle) = start_server(dir.path(), storage, addr);
                let client = rt.block_on(async {
                    let client = Client::connect(addr, 10);
                    set_keys(client.clone(), ops.clone()).await;
                    client
                });

                // Benchmark
                b.to_async(&rt)
                    .iter(|| get_keys(client.clone(), ops.clone()));

                // Teardown
                tx.send(()).unwrap();
                handle.join().unwrap();
            },
        );
    }
    group.finish();
}

fn set_bench(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let storage_types = &["bitcask", "sled"];

    let mut group = c.benchmark_group("set_bench");

    for &storage in storage_types {
        group.bench_with_input(
            BenchmarkId::new("set_bench", format!("{:?}", storage)),
            &storage,
            |b, &storage| {
                // Setup
                let dir = TempDir::new().unwrap();
                let addr: SocketAddr = ADDR.parse().unwrap();
                let ops: Vec<u64> = (0..NUM_OPS).collect();
                let (tx, handle) = start_server(dir.path(), storage, addr);
                let client = Client::connect(addr, 10);

                // Benchmark
                b.to_async(&rt)
                    .iter(|| set_keys(client.clone(), ops.clone()));

                // Teardown
                tx.send(()).unwrap();
                handle.join().unwrap();
            },
        );
    }
    group.finish();
}

fn get_and_set_bench(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let storage_types = &["bitcask", "sled"];

    let mut group = c.benchmark_group("get_and_set_bench");

    for &storage in storage_types {
        group.bench_with_input(
            BenchmarkId::new("get_and_set_bench", format!("{:?}", storage)),
            &storage,
            |b, &storage| {
                // Setup
                let dir = TempDir::new().unwrap();
                let addr: SocketAddr = ADDR.parse().unwrap();
                let ops: Vec<u64> = (0..NUM_OPS).collect();
                let (tx, handle) = start_server(dir.path(), storage, addr);
                let client = Client::connect(addr, 10);

                // Benchmark
                b.to_async(&rt)
                    .iter(|| get_and_set_keys(client.clone(), ops.clone()));

                // Teardown
                tx.send(()).unwrap();
                handle.join().unwrap();
            },
        );
    }
    group.finish();
}

async fn set_keys(client: smoldb::Client, keys: Vec<u64>) {
    let tasks: Vec<_> = keys
        .into_iter()
        .map(|i| {
            let client_clone = client.clone();
            task::spawn(async move {
                client_clone
                    .set(format!("key{}", i), "value".to_string())
                    .await
                    .unwrap();
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
}

async fn get_keys(client: smoldb::Client, keys: Vec<u64>) {
    let tasks: Vec<_> = keys
        .into_iter()
        .map(|i| {
            let client_clone = client.clone();
            task::spawn(async move {
                let val = client_clone.get(format!("key{}", i)).await.unwrap();
                assert_eq!(val, Some("value".to_string()));
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
}

async fn get_and_set_keys(client: smoldb::Client, keys: Vec<u64>) {
    let tasks: Vec<_> = keys
        .into_iter()
        .map(|i| {
            let client_clone = client.clone();
            task::spawn(async move {
                client_clone
                    .set(format!("key{}", i), "value".to_string())
                    .await
                    .unwrap();
                let val = client_clone.get(format!("key{}", i)).await.unwrap();
                assert_eq!(val, Some("value".to_string()));
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
}

fn start_server(
    dir: &Path,
    storage_type: &str,
    addr: SocketAddr,
) -> (SyncSender<()>, JoinHandle<()>) {
    let (tx, rx) = mpsc::sync_channel(0);
    let mut server = Command::cargo_bin("smoldb").unwrap();
    let mut child = server
        .args(&[
            "--storage",
            storage_type,
            "--addr",
            addr.to_string().as_str(),
        ])
        .current_dir(dir)
        .spawn()
        .unwrap();
    let handle = thread::spawn(move || {
        let _ = rx.recv(); // wait for main thread to finish
        child.kill().expect("server exited before killed");
    });
    thread::sleep(Duration::from_secs(1));
    (tx, handle)
}

criterion_group!(benches, get_bench, set_bench, get_and_set_bench);
criterion_main!(benches);
