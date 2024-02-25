use std::{env::current_dir, net::SocketAddr};

use clap::{Parser, ValueEnum};
use smoldb::{Bitcask, Server, ServerResult, Sled, Storage};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

const DEFAULT_ADDR: &str = "127.0.0.1:4001";

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(short, long)]
    storage: Option<StorageType>,

    #[arg(short, long, default_value = DEFAULT_ADDR)]
    addr: SocketAddr,
}

#[derive(Debug, Copy, Clone, PartialEq, ValueEnum)]
enum StorageType {
    Bitcask,
    Sled,
}

fn main() -> ServerResult<()> {
    init_tracing();

    let cli = Cli::parse();
    let addr = cli.addr;
    let storage_type = cli.storage.unwrap_or(StorageType::Bitcask);

    info!("smoldb {}", env!("CARGO_PKG_VERSION"));
    info!("storage type: {:?}", storage_type);
    info!("listening on {}", addr);

    match storage_type {
        StorageType::Bitcask => run(Bitcask::open(current_dir()?)?, addr),
        StorageType::Sled => run(Sled::open(current_dir()?)?, addr),
    }
}

fn run<T: Storage>(storage: T, addr: SocketAddr) -> ServerResult<()> {
    let mut server = Server::new(storage);
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
