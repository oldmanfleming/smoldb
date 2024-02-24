use std::{
    io::Write,
    net::{SocketAddr, TcpStream},
};

use clap::{Args, Parser, Subcommand};
use thiserror::Error;

const DEFAULT_ADDR: &str = "127.0.0.1:4001";

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Command,

    #[arg(short, long, value_enum, default_value = DEFAULT_ADDR)]
    addr: SocketAddr,
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

#[derive(Error, Debug)]
enum ClientError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

fn main() -> Result<(), ClientError> {
    let cli = Cli::parse();

    let mut stream = TcpStream::connect(cli.addr)?;

    match cli.command {
        Command::Get(GetCommand { key }) => {
            let message = format!("GET {}", key);
            stream.write_all(message.as_bytes())?;
            stream.flush()?;
        }
        Command::Set(SetCommand {
            key: _key,
            value: _value,
        }) => {
            todo!();
        }
        Command::Remove(RemoveCommand { key: _key }) => {
            todo!();
        }
        Command::Merge => {
            todo!();
        }
        Command::List => {
            todo!();
        }
    }

    Ok(())
}
