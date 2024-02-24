use std::{
    env::current_dir,
    io::{self, Read},
    net::{SocketAddr, TcpListener, TcpStream},
};

use clap::{Parser, ValueEnum};
use smoldb::{Bitcask, Storage, StorageError, StorageResult};
use thiserror::Error;
use tracing::{error, info, Level};
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

#[derive(Error, Debug)]
enum ServerError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Storage error: {0}")]
    StorageError(#[from] StorageError),
}

fn main() -> Result<(), ServerError> {
    init_tracing();

    let cli = Cli::parse();
    let addr = cli.addr;
    let storage = cli.storage.unwrap_or(StorageType::Bitcask);

    info!("smoldb {}", env!("CARGO_PKG_VERSION"));
    info!("storage engine: {:?}", storage);
    info!("listening on {}", addr);

    match storage {
        StorageType::Bitcask => run(Bitcask::open(current_dir()?)?, addr),
        StorageType::Sled => unimplemented!(),
        StorageType::Memory => unimplemented!(),
    }
}

fn run<E: Storage>(mut engine: E, addr: SocketAddr) -> Result<(), ServerError> {
    let listener = TcpListener::bind(addr)?;

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                if let Err(e) = serve(&mut engine, stream) {
                    error!("Error serving connection: {}", e);
                }
            }
            Err(err) => error!("Connection failed: {}", err),
        }
    }

    Ok(())
}

fn serve<E: Storage>(engine: &mut E, mut stream: TcpStream) -> StorageResult<()> {
    let mut buffer = [0; 1024];
    stream.read(&mut buffer)?;

    let request = String::from_utf8(buffer.to_vec())?;

    info!("received request: {}", request);
    info!("{}", engine.get(String::from("test2")).unwrap().unwrap());

    Ok(())
}

fn init_tracing() {
    let subscriber = FmtSubscriber::builder()
        .with_writer(std::io::stderr)
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();
}
