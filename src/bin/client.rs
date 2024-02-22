use std::net::SocketAddr;

use clap::{Args, Parser, Subcommand};

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

fn main() {
    let cli = Cli::parse();

    let _addr = cli.addr;

    match cli.command {
        Command::Get(GetCommand { key }) => {
            todo!();
        }
        Command::Set(SetCommand { key, value }) => {
            todo!();
        }
        Command::Remove(RemoveCommand { key }) => {
            todo!();
        }
        Command::Merge => {
            todo!();
        }
        Command::List => {
            todo!();
        }
    }
}
