use std::{env::current_dir, net::SocketAddr};

use clap::{Parser, ValueEnum};
use smoldb::{Bitcask, Server, ServerResult};
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
    Memory,
}

fn main() -> ServerResult<()> {
    init_tracing();

    let cli = Cli::parse();
    let addr = cli.addr;
    let storage = cli.storage.unwrap_or(StorageType::Bitcask);

    info!("smoldb {}", env!("CARGO_PKG_VERSION"));
    info!("storage type: {:?}", storage);
    info!("listening on {}", addr);

    let mut server = match storage {
        StorageType::Bitcask => Server::new(Bitcask::open(current_dir()?)?),
        StorageType::Sled => unimplemented!(),
        StorageType::Memory => unimplemented!(),
    };

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
