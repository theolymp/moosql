use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde::Deserialize;
use std::path::PathBuf;

/// moo: Copy-on-Write proxy for MariaDB/MySQL
#[derive(Parser, Debug)]
#[command(name = "moo", author, version, about = "Copy-on-Write proxy for MariaDB/MySQL", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Start the moo proxy
    Start(StartArgs),
    /// Show proxy status
    Status(StatusArgs),
    /// Reset overlay data
    Reset(ResetArgs),
    /// List tracked tables
    Tables(TablesArgs),
    /// Show what changed in the overlay compared to the base database
    Diff(DiffArgs),
    /// Save a named snapshot of the overlay
    Snapshot(SnapshotArgs),
    /// Restore a named snapshot to the overlay
    Restore(RestoreArgs),
    /// List saved snapshots
    Snapshots(SnapshotsArgs),
    /// Manage named overlays (create, list, switch, delete, branch, merge)
    Overlay(OverlayArgs),
    /// Apply overlay changes to the upstream database
    Apply(ApplyArgs),
    /// Compare two overlay directories (like git diff branch-a branch-b)
    DiffOverlays(DiffOverlaysArgs),
}

#[derive(clap::Args, Debug)]
pub struct StartArgs {
    /// Upstream MariaDB/MySQL address
    #[arg(long, default_value = "localhost:3306")]
    pub upstream: String,

    /// Address to listen on
    #[arg(long, default_value = "localhost:3307")]
    pub listen: String,

    /// Path to the overlay directory
    #[arg(long, default_value = "./dev-overlay")]
    pub overlay: PathBuf,

    /// Database user (env: MOO_UPSTREAM_USER)
    #[arg(long, env = "MOO_UPSTREAM_USER")]
    pub user: Option<String>,

    /// Database password (env: MOO_UPSTREAM_PASSWORD)
    #[arg(long, env = "MOO_UPSTREAM_PASSWORD")]
    pub password: Option<String>,

    /// Named overlay to use within the overlay base directory (default: "default")
    #[arg(long, default_value = "default")]
    pub overlay_name: String,

    /// Path to config file
    #[arg(long)]
    pub config: Option<PathBuf>,

    /// Enable auth passthrough (clients provide their own credentials, per-user overlay isolation)
    #[arg(long)]
    pub auth_passthrough: bool,

    /// Enable live query logging to stdout
    #[arg(long)]
    pub watch: bool,

    /// Filter watch output (e.g., INSERT, SELECT, UPDATE, DELETE, or table name)
    #[arg(long)]
    pub watch_filter: Option<String>,
}

#[derive(clap::Args, Debug)]
pub struct StatusArgs {
    /// Path to the overlay directory
    #[arg(long, default_value = "./dev-overlay")]
    pub overlay: PathBuf,
}

#[derive(clap::Args, Debug)]
pub struct ResetArgs {
    /// Path to the overlay directory
    #[arg(long, default_value = "./dev-overlay")]
    pub overlay: PathBuf,

    /// Optional table name to reset (resets all if omitted)
    pub table: Option<String>,
}

#[derive(clap::Args, Debug)]
pub struct TablesArgs {
    /// Path to the overlay directory
    #[arg(long, default_value = "./dev-overlay")]
    pub overlay: PathBuf,
}

#[derive(clap::Args, Debug)]
pub struct DiffArgs {
    /// Path to the overlay directory
    #[arg(long, default_value = "./dev-overlay")]
    pub overlay: PathBuf,

    /// Output format: text (default), sql
    #[arg(long, default_value = "text")]
    pub format: String,

    /// Show row-level details (default: table summary only)
    #[arg(long)]
    pub verbose: bool,

    /// For UPDATEs: fetch base rows and show old->new diff
    #[arg(long)]
    pub full: bool,

    /// Upstream address (required with --full)
    #[arg(long)]
    pub upstream: Option<String>,

    /// Upstream user (required with --full, env: MOO_UPSTREAM_USER)
    #[arg(long, env = "MOO_UPSTREAM_USER")]
    pub user: Option<String>,

    /// Upstream password (required with --full, env: MOO_UPSTREAM_PASSWORD)
    #[arg(long, env = "MOO_UPSTREAM_PASSWORD")]
    pub password: Option<String>,

    /// Filter to a specific table
    #[arg(long)]
    pub table: Option<String>,

    /// Filter to a specific database (default: all)
    #[arg(long)]
    pub db: Option<String>,
}

#[derive(clap::Args, Debug)]
pub struct SnapshotArgs {
    /// Name for the snapshot
    pub name: String,

    /// Path to the overlay directory
    #[arg(long, default_value = "./dev-overlay")]
    pub overlay: PathBuf,

    /// Overwrite an existing snapshot with the same name
    #[arg(long)]
    pub force: bool,
}

#[derive(clap::Args, Debug)]
pub struct RestoreArgs {
    /// Name of the snapshot to restore
    pub name: String,

    /// Path to the overlay directory
    #[arg(long, default_value = "./dev-overlay")]
    pub overlay: PathBuf,
}

#[derive(clap::Args, Debug)]
pub struct SnapshotsArgs {
    /// Path to the overlay directory
    #[arg(long, default_value = "./dev-overlay")]
    pub overlay: PathBuf,
}

#[derive(clap::Args, Debug)]
pub struct OverlayArgs {
    /// Base directory containing named overlays
    #[arg(long, default_value = "./dev-overlay")]
    pub base: PathBuf,

    #[command(subcommand)]
    pub subcommand: OverlaySubcommand,
}

#[derive(Subcommand, Debug)]
pub enum OverlaySubcommand {
    /// Create a new empty overlay
    Create {
        /// Name of the overlay to create
        name: String,
    },
    /// List all overlays, marking the active one
    List,
    /// Switch to a different overlay (proxy must be restarted)
    Switch {
        /// Name of the overlay to switch to
        name: String,
    },
    /// Delete an overlay
    Delete {
        /// Name of the overlay to delete
        name: String,
    },
    /// Show which overlay is currently active
    Active,
    /// Copy an overlay as the basis for a new one
    Branch {
        /// Name of the source overlay
        source: String,
        /// Name for the new overlay
        new_name: String,
    },
    /// Merge source overlay into target (reports conflicts, does not auto-resolve)
    Merge {
        /// Name of the source overlay
        source: String,
        /// Name of the target overlay
        target: String,
    },
}

#[derive(clap::Args, Debug)]
pub struct ApplyArgs {
    /// Path to the overlay directory
    #[arg(long, default_value = "./dev-overlay")]
    pub overlay: PathBuf,

    /// Upstream MariaDB address (host:port)
    #[arg(long)]
    pub upstream: String,

    /// Upstream database user (env: MOO_UPSTREAM_USER)
    #[arg(long, env = "MOO_UPSTREAM_USER")]
    pub user: String,

    /// Upstream database password (env: MOO_UPSTREAM_PASSWORD)
    #[arg(long, env = "MOO_UPSTREAM_PASSWORD", default_value = "")]
    pub password: String,

    /// Apply only this database (default: all databases in overlay)
    #[arg(long)]
    pub db: Option<String>,

    /// Apply only this table (default: all tables)
    #[arg(long)]
    pub table: Option<String>,

    /// Show what would be applied without executing
    #[arg(long)]
    pub dry_run: bool,

    /// Skip confirmation prompt
    #[arg(long)]
    pub yes: bool,

    /// Reset the overlay after a successful apply
    #[arg(long)]
    pub reset: bool,
}

#[derive(clap::Args, Debug)]
pub struct DiffOverlaysArgs {
    /// Path to the first overlay directory
    pub overlay_a: PathBuf,

    /// Path to the second overlay directory
    pub overlay_b: PathBuf,

    /// Filter to a specific database
    #[arg(long)]
    pub db: Option<String>,

    /// Filter to a specific table
    #[arg(long)]
    pub table: Option<String>,
}

/// Top-level config file structure
#[allow(dead_code)]
#[derive(Debug, Deserialize, Default)]
pub struct ConfigFile {
    pub upstream: Option<UpstreamConfig>,
    pub proxy: Option<ProxyConfig>,
    pub overlay: Option<OverlayConfig>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct UpstreamConfig {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub user: Option<String>,
    pub password: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct ProxyConfig {
    pub listen: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct OverlayConfig {
    pub path: Option<PathBuf>,
}

impl ConfigFile {
    pub fn load(path: &PathBuf) -> Result<Self> {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;
        let config: ConfigFile = toml::from_str(&contents)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;
        Ok(config)
    }
}
