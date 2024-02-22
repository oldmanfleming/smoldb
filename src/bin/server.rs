use std::{env::current_dir, net::SocketAddr};

use clap::{Parser, ValueEnum};
use smoldb::{Bitcask, Storage, StorageResult};
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

impl std::fmt::Display for StorageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_possible_value()
            .expect("no values are skipped")
            .get_name()
            .fmt(f)
    }
}

fn main() -> StorageResult<()> {
    init_tracing();

    let cli = Cli::parse();
    let addr = cli.addr;
    let storage = cli.storage.unwrap_or(StorageType::Bitcask);

    info!("smoldb {}", env!("CARGO_PKG_VERSION"));
    info!("storage engine: {}", storage);
    info!("listening on {}", addr);

    match storage {
        StorageType::Bitcask => run_with_engine(Bitcask::open(current_dir()?)?, addr),
        StorageType::Sled => unimplemented!(),
        StorageType::Memory => unimplemented!(),
    }
}

fn run_with_engine<E: Storage>(mut engine: E, _addr: SocketAddr) -> StorageResult<()> {
    println!("{}", engine.get(String::from("test2")).unwrap().unwrap());

    Ok(())
}

fn init_tracing() {
    let subscriber = FmtSubscriber::builder()
        .with_writer(std::io::stderr)
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();
}
