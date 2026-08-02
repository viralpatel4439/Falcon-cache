#![forbid(unsafe_code)]

mod cli;
mod client;
mod config_cmd;
mod serve;

use clap::Parser;
use cli::{Cli, Command};

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let profile = cli.profile;

    match cli.command {
        // Profile management (no running node needed).
        Command::Status => config_cmd::status(&profile),
        Command::Config(c) => config_cmd::config(&profile, c),

        // Run a node from the profile.
        Command::Serve(args) => serve::run(&profile, args),

        // Client subcommands: talk to a running node over HTTP.
        Command::Get(a) => client::get(&profile, a),
        Command::Put(a) => client::put(&profile, a),
        Command::Del(a) => client::del(&profile, a),
        Command::Health(a) => client::health(&profile, a),
    }
}
