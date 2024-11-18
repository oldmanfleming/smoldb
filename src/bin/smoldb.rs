use std::{env::current_dir, net::SocketAddr};

use clap::{Parser, ValueEnum};
use smoldb::{
    Bitcask, NaiveThreadPool, RayonThreadPool, Server, ServerResult, SharedQueueThreadPool, Sled,
    Storage, ThreadPool,
};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

const DEFAULT_ADDR: &str = "127.0.0.1:4001";

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(short, long)]
    storage: Option<StorageType>,

    #[arg(short, long)]
    pool: Option<ThreadPoolType>,

    #[arg(long)]
    pool_size: Option<u32>,

    #[arg(short, long, default_value = DEFAULT_ADDR)]
    addr: SocketAddr,
}

#[derive(Debug, Copy, Clone, PartialEq, ValueEnum)]
enum StorageType {
    Bitcask,
    Sled,
}

#[derive(Debug, Copy, Clone, PartialEq, ValueEnum)]
enum ThreadPoolType {
    Naive,
    Rayon,
    SharedQueue,
}

fn main() -> ServerResult<()> {
    init_tracing();

    let cli = Cli::parse();
    let addr = cli.addr;
    let storage_type = cli.storage.unwrap_or(StorageType::Bitcask);
    let thread_pool_type = cli.pool.unwrap_or(ThreadPoolType::Naive);
    let thread_pool_size = cli.pool_size.unwrap_or(32);
    let current_dir = current_dir()?;

    info!("smoldb {}", env!("CARGO_PKG_VERSION"));
    info!("storage type: {:?}", storage_type);
    info!(
        "thread pool type: {:?} with size: {}",
        thread_pool_type, thread_pool_size
    );
    info!("working directory: {:?}", current_dir);
    info!("listening on {}", addr);

    match (storage_type, thread_pool_type) {
        (StorageType::Bitcask, ThreadPoolType::Naive) => run(
            Bitcask::open(current_dir)?,
            NaiveThreadPool::new(thread_pool_size)?,
            addr,
        ),
        (StorageType::Sled, ThreadPoolType::Naive) => run(
            Sled::open(current_dir)?,
            NaiveThreadPool::new(thread_pool_size)?,
            addr,
        ),
        (StorageType::Bitcask, ThreadPoolType::Rayon) => run(
            Bitcask::open(current_dir)?,
            RayonThreadPool::new(thread_pool_size)?,
            addr,
        ),
        (StorageType::Sled, ThreadPoolType::Rayon) => run(
            Sled::open(current_dir)?,
            RayonThreadPool::new(thread_pool_size)?,
            addr,
        ),
        (StorageType::Bitcask, ThreadPoolType::SharedQueue) => run(
            Bitcask::open(current_dir)?,
            SharedQueueThreadPool::new(thread_pool_size)?,
            addr,
        ),
        (StorageType::Sled, ThreadPoolType::SharedQueue) => run(
            Sled::open(current_dir)?,
            SharedQueueThreadPool::new(thread_pool_size)?,
            addr,
        ),
    }
}

fn run<S: Storage, T: ThreadPool>(
    storage: S,
    thread_pool: T,
    addr: SocketAddr,
) -> ServerResult<()> {
    let mut server = Server::new(storage, thread_pool);
    server.run(addr)
}

#[cfg(debug_assertions)]
fn init_tracing() {
    let subscriber = FmtSubscriber::builder()
        .with_writer(std::io::stderr)
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();
}

#[cfg(not(debug_assertions))]
fn init_tracing() {
    let subscriber = FmtSubscriber::builder()
        .with_writer(std::io::stderr)
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();
}
