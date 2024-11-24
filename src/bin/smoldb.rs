use std::{env::current_dir, net::SocketAddr};
use tokio::signal;
use tokio::sync::oneshot;

use clap::{Parser, ValueEnum};
use smoldb::{run, ServerResult, StorageType};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

const DEFAULT_ADDR: &str = "127.0.0.1:4001";

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(short, long)]
    storage: Option<CliStorageType>,

    #[arg(short, long, default_value = DEFAULT_ADDR)]
    addr: SocketAddr,
}

#[derive(Debug, Copy, Clone, PartialEq, ValueEnum)]
enum CliStorageType {
    Bitcask,
    Sled,
}

#[tokio::main]
async fn main() -> ServerResult<()> {
    init_tracing();

    let cli = Cli::parse();
    let addr = cli.addr;
    let storage_type = cli.storage.unwrap_or(CliStorageType::Bitcask);
    let current_dir = current_dir()?;

    info!("smoldb {}", env!("CARGO_PKG_VERSION"));
    info!("storage type: {:?}", storage_type);
    info!("working directory: {:?}", current_dir);

    let (stop_tx, stop_rx) = oneshot::channel();

    tokio::spawn(async move {
        signal::ctrl_c().await.expect("failed to listen for event");
        info!("shutting down server");
        stop_tx.send(()).expect("failed to send stop signal");
    });

    info!("listening on {}", addr);

    match storage_type {
        CliStorageType::Bitcask => run(addr, current_dir, StorageType::Bitcask, stop_rx).await?,
        CliStorageType::Sled => run(addr, current_dir, StorageType::Sled, stop_rx).await?,
    };

    info!("server stopped");

    Ok(())
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
