use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use rand::prelude::*;
use smoldb::{Bitcask, Sled, Storage};
use tempfile::TempDir;

fn set_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_bench");
    group.bench_function("bitcask", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                (Bitcask::open(temp_dir.path()).unwrap(), temp_dir)
            },
            |(store, _temp_dir)| {
                for i in 1..(1 << 12) {
                    store.set(format!("key{}", i), "value".to_string()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("sled", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                (Sled::open(temp_dir.path()).unwrap(), temp_dir)
            },
            |(db, _temp_dir)| {
                for i in 1..(1 << 12) {
                    db.set(format!("key{}", i), "value".to_string()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn get_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_bench");
    for i in &vec![8, 12, 16, 20] {
        group.bench_with_input(format!("bitcask_{}", i), i, |b, i| {
            let temp_dir = TempDir::new().unwrap();
            let store = Bitcask::open(temp_dir.path()).unwrap();
            for key_i in 1..(1 << i) {
                store
                    .set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            let mut rng = SmallRng::from_seed([0; 32]);
            b.iter(|| {
                store
                    .get(format!("key{}", rng.gen_range(1..1 << i)))
                    .unwrap();
            })
        });
    }
    for i in &vec![8, 12, 16, 20] {
        group.bench_with_input(format!("sled_{}", i), i, |b, i| {
            let temp_dir = TempDir::new().unwrap();
            let db = Sled::open(temp_dir.path()).unwrap();
            for key_i in 1..(1 << i) {
                db.set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            let mut rng = SmallRng::from_seed([0; 32]);
            b.iter(|| {
                db.get(format!("key{}", rng.gen_range(1..1 << i))).unwrap();
            })
        });
    }
    group.finish();
}

criterion_group!(benches, set_bench, get_bench);
criterion_main!(benches);
