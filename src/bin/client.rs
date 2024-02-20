use clap::{Args, Parser, Subcommand};
use smoldb::{Bitcask, Storage, StorageError, StorageResult};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    #[command(name = "get", about = "Get the string value of a given string key")]
    Get(GetCommand),
    #[command(name = "set", about = "Set the value of a string key to a string")]
    Set(SetCommand),
    #[command(name = "rm", about = "Remove a given key")]
    Remove(RemoveCommand),
    #[command(name = "merge", about = "Compact log files and remove stale data")]
    Merge,
    #[command(name = "ls", about = "List all keys")]
    List,
}

#[derive(Args, Debug)]
struct GetCommand {
    #[arg(name = "KEY", help = "A string key")]
    key: String,
}

#[derive(Args, Debug)]
struct SetCommand {
    #[arg(name = "KEY", help = "A string key")]
    key: String,
    #[arg(name = "VALUE", help = "A string value")]
    value: String,
}

#[derive(Args, Debug)]
struct RemoveCommand {
    #[arg(name = "KEY", help = "A string key")]
    key: String,
}

fn main() -> StorageResult<()> {
    let cli = Cli::parse();

    let mut bitcask = Bitcask::open(std::env::current_dir()?)?;

    Ok(match cli.command {
        Command::Get(GetCommand { key }) => {
            if let Some(value) = bitcask.get(key)? {
                println!("{}", value)
            } else {
                println!("Key not found")
            }
        }
        Command::Set(SetCommand { key, value }) => {
            bitcask.set(key, value)?;
        }
        Command::Remove(RemoveCommand { key }) => match bitcask.remove(key) {
            Ok(_) => {}
            Err(StorageError::KeyNotFound) => {
                println!("Key not found");
                std::process::exit(1);
            }
            Err(err) => Err(err)?,
        },
        Command::Merge => {
            bitcask.merge()?;
        }
        Command::List => {
            for key in bitcask.list_keys() {
                println!("{}", key);
            }
        }
    })
}
