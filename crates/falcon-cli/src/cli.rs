use clap::{Args, Parser, Subcommand};

/// Falcon Cache — an in-memory cache with TTL and a hard memory bound.
///
/// Run it, then use it:
///   falcon serve                    # run the node (UI at :8080)
///   falcon put k v --ttl 60         # from another shell
///
/// Falcon is configured ONLY through this CLI — it never reads environment
/// variables. `falcon config set <key> <value>` edits your profile.
#[derive(Parser, Debug)]
#[command(name = "falcon", version, about, long_about = None)]
pub struct Cli {
    /// Path to the profile file (default: ~/.falcon/profile.toml). A flag, not
    /// an env var — Falcon never reads the environment for configuration.
    #[arg(long, global = true)]
    pub profile: Option<String>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Show the node's current configuration and build.
    Status,

    /// View or change configuration (the CLI/UI-only config path).
    #[command(subcommand)]
    Config(ConfigCmd),

    /// Run this node using the profile.
    Serve(ServeArgs),

    // --- Client subcommands: talk to a running node over HTTP ---
    /// Get a key's value from a running node.
    Get(KeyArgs),
    /// Put (set) a key's value. Value is read from the arg or stdin.
    Put(PutArgs),
    /// Delete a key.
    Del(KeyArgs),

    /// Print a node's health JSON.
    Health(ClientArgs),
}

#[derive(Subcommand, Debug)]
pub enum ConfigCmd {
    /// Set a config key in the profile, e.g. `falcon config set capacity-mb 512`.
    Set { key: String, value: String },
    /// Print one config value.
    Get { key: String },
    /// List every config key and its current value.
    List,
}

/// Options for `falcon serve`. Every field overrides the profile for ONE run;
/// the profile (written by `config set`) remains the durable source of truth.
/// None of these are environment variables.
#[derive(Args, Debug, Default)]
pub struct ServeArgs {
    /// Advanced/testing escape hatch: load a full engine config TOML directly,
    /// bypassing the profile. Lets you declare arbitrary keyspaces for one run
    /// (used by the benchmark harness).
    #[arg(long)]
    pub config: Option<String>,
    /// HTTP/UI bind address (overrides the profile for this run).
    #[arg(long)]
    pub http_bind: Option<String>,
    /// Binary wire-protocol bind address.
    #[arg(long)]
    pub wire_bind: Option<String>,
    /// Enable the fast binary protocol server for this run.
    #[arg(long)]
    pub wire_enabled: bool,
    /// Disable the fast binary protocol server for this run.
    #[arg(long, conflicts_with = "wire_enabled")]
    pub wire_disabled: bool,
    /// Max RAM (MB) the cache may hold — a hard bound; it evicts rather than
    /// exceed it. Omit to size from the memory this process actually has.
    #[arg(long)]
    pub capacity_mb: Option<usize>,
    /// Default TTL in seconds for writes that don't specify one (0 = no expiry).
    #[arg(long)]
    pub default_ttl: Option<u64>,
    #[arg(long)]
    pub node_id: Option<String>,
    #[arg(long)]
    pub region: Option<String>,
    #[arg(long, default_value = "info")]
    pub log_level: String,
}

/// Options shared by every client subcommand. Addresses are flags (with a
/// sensible default), never environment variables.
#[derive(Args, Debug, Clone)]
pub struct ClientArgs {
    /// Base URL of the node's HTTP API.
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    pub addr: String,
    /// API key, if the node has auth enabled.
    #[arg(long)]
    pub api_key: Option<String>,
}

#[derive(Args, Debug, Clone)]
pub struct KeyArgs {
    pub key: String,
    #[command(flatten)]
    pub client: ClientArgs,
}

#[derive(Args, Debug, Clone)]
pub struct PutArgs {
    pub key: String,
    /// The value. If omitted, read from stdin.
    pub value: Option<String>,
    /// Optional TTL in seconds.
    #[arg(long)]
    pub ttl: Option<u64>,
    #[command(flatten)]
    pub client: ClientArgs,
}
