use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    #[command(name = "get")]
    Get(GetCommand),
    #[command(name = "set")]
    Set(SetCommand),
    #[command(name = "rm")]
    Remove(RemoveCommand),
}

#[derive(Args, Debug)]
struct GetCommand {
    key: String,
}

#[derive(Args, Debug)]
struct SetCommand {
    key: String,
    value: String,
}

#[derive(Args, Debug)]
struct RemoveCommand {
    key: String,
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Get(GetCommand { .. }) => {
            eprintln!("unimplemented");
            std::process::exit(1);
        }
        Command::Set(SetCommand { .. }) => {
            eprintln!("unimplemented");
            std::process::exit(1);
        }
        Command::Remove(RemoveCommand { .. }) => {
            eprintln!("unimplemented");
            std::process::exit(1);
        }
    }
}
