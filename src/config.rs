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

    /// Database user
    #[arg(long)]
    pub user: Option<String>,

    /// Database password
    #[arg(long)]
    pub password: Option<String>,

    /// Path to config file
    #[arg(long)]
    pub config: Option<PathBuf>,
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
