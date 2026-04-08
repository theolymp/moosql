mod bridge;
mod cli;
mod config;
mod overlay;
mod protocol;
mod proxy;
mod sql;

use anyhow::Result;
use clap::Parser;
use config::{Cli, Commands, ConfigFile};
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Start(args) => {
            // Load optional config file
            let file_config = if let Some(ref config_path) = args.config {
                info!(path = %config_path.display(), "Loading config file");
                match ConfigFile::load(config_path) {
                    Ok(cfg) => {
                        info!("Config file loaded successfully");
                        cfg
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to load config file, using defaults");
                        ConfigFile::default()
                    }
                }
            } else {
                ConfigFile::default()
            };

            info!(?file_config, "Config file values");

            // Resolve credentials: CLI args take priority over config file.
            let upstream_user = args
                .user
                .or_else(|| {
                    file_config
                        .upstream
                        .as_ref()
                        .and_then(|u| u.user.clone())
                })
                .unwrap_or_else(|| "root".to_string());

            let upstream_password = args
                .password
                .or_else(|| {
                    file_config
                        .upstream
                        .as_ref()
                        .and_then(|u| u.password.clone())
                })
                .unwrap_or_default();

            info!(user = %upstream_user, "Using upstream credentials");

            let server = proxy::server::ProxyServer::new(
                args.listen,
                args.upstream,
                upstream_user,
                upstream_password,
                args.overlay,
            );
            return server.run().await;
        }

        Commands::Status(args) => cli::commands::print_status(&args.overlay)?,

        Commands::Reset(args) => {
            cli::commands::reset_overlay(&args.overlay, args.table.as_deref())?;
            println!("Overlay reset.");
        }

        Commands::Tables(args) => cli::commands::print_tables(&args.overlay)?,
    }

    Ok(())
}
