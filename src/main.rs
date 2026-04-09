#![allow(dead_code)]

mod bridge;
mod cli;
mod config;
mod overlay;
mod protocol;
mod proxy;
mod sql;

use anyhow::Result;
use clap::Parser;
use cli::apply::run_apply;
use cli::diff_overlays::run_diff_overlays;
use config::{Cli, Commands, ConfigFile, OverlaySubcommand};
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

            // Resolve the actual overlay directory: support named multi-overlay layout.
            let overlay_dir = cli::overlay_mgmt::resolve_overlay_dir(&args.overlay);
            info!(overlay_dir = %overlay_dir.display(), "Using overlay directory");

            let server = proxy::server::ProxyServer::new(
                args.listen,
                args.upstream,
                upstream_user,
                upstream_password,
                overlay_dir,
                args.watch,
                args.watch_filter,
            );
            return server.run().await;
        }

        Commands::Status(args) => {
            let overlay_dir = cli::overlay_mgmt::resolve_overlay_dir(&args.overlay);
            cli::commands::print_status(&overlay_dir)?;
        }

        Commands::Reset(args) => {
            let overlay_dir = cli::overlay_mgmt::resolve_overlay_dir(&args.overlay);
            cli::commands::reset_overlay(&overlay_dir, args.table.as_deref())?;
            println!("Overlay reset.");
        }

        Commands::Tables(args) => {
            let overlay_dir = cli::overlay_mgmt::resolve_overlay_dir(&args.overlay);
            cli::commands::print_tables(&overlay_dir)?;
        }

        Commands::Diff(args) => {
            let format = match args.format.as_str() {
                "sql" => cli::diff::DiffFormat::Sql,
                _ => cli::diff::DiffFormat::Text,
            };
            let resolved = cli::overlay_mgmt::resolve_overlay_dir(&args.overlay);
            cli::diff::run_diff(
                &resolved,
                format,
                args.verbose,
                args.full,
                args.upstream.as_deref(),
                args.user.as_deref(),
                args.password.as_deref(),
                args.db.as_deref(),
                args.table.as_deref(),
            )
            .await?;
        }

        Commands::Snapshot(args) => {
            cli::snapshot::save_snapshot(&args.overlay, &args.name, args.force)?;
        }

        Commands::Restore(args) => {
            cli::snapshot::restore_snapshot(&args.overlay, &args.name)?;
        }

        Commands::Snapshots(args) => {
            cli::snapshot::list_snapshots(&args.overlay)?;
        }

        Commands::Overlay(args) => {
            let base = &args.base;
            match args.subcommand {
                OverlaySubcommand::Create { name } => {
                    cli::overlay_mgmt::create_overlay(base, &name)?;
                }
                OverlaySubcommand::List => {
                    cli::overlay_mgmt::list_overlays(base)?;
                }
                OverlaySubcommand::Switch { name } => {
                    cli::overlay_mgmt::switch_overlay(base, &name)?;
                }
                OverlaySubcommand::Delete { name } => {
                    cli::overlay_mgmt::delete_overlay(base, &name)?;
                }
                OverlaySubcommand::Active => {
                    cli::overlay_mgmt::show_active(base)?;
                }
                OverlaySubcommand::Branch { source, new_name } => {
                    cli::overlay_branch::branch_overlay(base, &source, &new_name)?;
                    println!("Branched overlay '{}' -> '{}'.", source, new_name);
                }
                OverlaySubcommand::Merge { source, target } => {
                    let conflicts =
                        cli::overlay_branch::merge_overlays(base, &source, &target)?;
                    if conflicts.is_empty() {
                        println!("Merge complete. No conflicts.");
                    } else {
                        println!(
                            "Merge complete with {} conflict(s):",
                            conflicts.len()
                        );
                        for c in &conflicts {
                            println!("  [{}/{}] pk={}", c.db_name, c.table_name, c.pk);
                        }
                        std::process::exit(1);
                    }
                }
            }
        }

        Commands::Apply(args) => {
            run_apply(
                &args.overlay,
                &args.upstream,
                &args.user,
                &args.password,
                args.db.as_deref(),
                args.table.as_deref(),
                args.dry_run,
                args.yes,
                args.reset,
            )
            .await?;
        }

        Commands::DiffOverlays(args) => {
            run_diff_overlays(
                &args.overlay_a,
                &args.overlay_b,
                args.db.as_deref(),
                args.table.as_deref(),
            )?;
        }
    }

    Ok(())
}
