use std::net::SocketAddr;

use clap::{Args, Parser, Subcommand};
use smoldb::{Client, ClientResult};

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
    #[command(name = "get", about = "Get the value of a given key")]
    Get(GetCommand),
    #[command(name = "set", about = "Set the value of a key")]
    Set(SetCommand),
    #[command(name = "rm", about = "Remove a given key")]
    Remove(RemoveCommand),
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

fn main() -> ClientResult<()> {
    let cli = Cli::parse();

    let mut client = Client::connect(&cli.addr)?;
    match cli.command {
        Command::Get(GetCommand { key }) => {
            if let Some(value) = client.get(key)? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Command::Set(SetCommand { key, value }) => {
            client.set(key, value)?;
        }
        Command::Remove(RemoveCommand { key }) => {
            client.remove(key)?;
        }
        Command::List => {
            let keys = client.list()?;
            for key in keys {
                println!("{}", key);
            }
        }
    };

    Ok(())
}
