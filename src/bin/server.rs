use std::env::current_dir;

use clap::{Parser, ValueEnum};
use smoldb::{Bitcask, Storage, StorageResult};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(short, long, value_enum)]
    storage: Option<StorageType>,
}

#[derive(Debug, Copy, Clone, PartialEq, ValueEnum)]
enum StorageType {
    Bitcask,
    Sled,
    Memory,
}

fn main() -> StorageResult<()> {
    let cli = Cli::parse();

    match cli.storage {
        Some(StorageType::Bitcask) | None => run_with_engine(Bitcask::open(current_dir()?)?),
        Some(StorageType::Sled) => unimplemented!(),
        Some(StorageType::Memory) => unimplemented!(),
    }
}

fn run_with_engine<E: Storage>(mut engine: E) -> StorageResult<()> {
    // temp
    println!("{}", engine.get(String::from("test2")).unwrap().unwrap());

    Ok(())
}
