use std::path::PathBuf;

use anyhow::Result;
use opensrv_mysql::AsyncMysqlIntermediary;
use tracing::{error, info};

use super::handler::CowHandler;

pub struct ProxyServer {
    pub listen_addr: String,
    pub upstream_addr: String,
    pub upstream_user: String,
    pub upstream_password: String,
    pub overlay_dir: PathBuf,
    pub auth_passthrough: bool,
    pub watch: bool,
    pub watch_filter: Option<String>,
}

impl ProxyServer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        listen_addr: impl Into<String>,
        upstream_addr: impl Into<String>,
        upstream_user: impl Into<String>,
        upstream_password: impl Into<String>,
        overlay_dir: impl Into<PathBuf>,
        auth_passthrough: bool,
        watch: bool,
        watch_filter: Option<String>,
    ) -> Self {
        Self {
            listen_addr: listen_addr.into(),
            upstream_addr: upstream_addr.into(),
            upstream_user: upstream_user.into(),
            upstream_password: upstream_password.into(),
            overlay_dir: overlay_dir.into(),
            auth_passthrough,
            watch,
            watch_filter,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let addr: std::net::SocketAddr = self.listen_addr.parse()
            .or_else(|_| format!("{}:3307", self.listen_addr).parse())
            .map_err(|e| anyhow::anyhow!("Invalid listen address '{}': {}", self.listen_addr, e))?;
        let socket = tokio::net::TcpSocket::new_v4()?;
        socket.set_reuseaddr(true)?;
        socket.bind(addr)?;
        let listener = socket.listen(128)?;
        info!(
            listen = %self.listen_addr,
            upstream = %self.upstream_addr,
            user = %self.upstream_user,
            "Proxy listening"
        );

        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (socket, peer_addr) = result?;
                    info!(%peer_addr, "Accepted connection");

                    let upstream_addr = self.upstream_addr.clone();
                    let upstream_user = self.upstream_user.clone();
                    let upstream_password = self.upstream_password.clone();
                    let overlay_dir = self.overlay_dir.clone();
                    let auth_passthrough = self.auth_passthrough;
                    let watch = self.watch;
                    let watch_filter = self.watch_filter.clone();

                    tokio::spawn(async move {
                        let handler = CowHandler::new(upstream_addr, upstream_user, upstream_password, overlay_dir, auth_passthrough, watch, watch_filter);

                        // Split the TCP stream into read/write halves for opensrv-mysql
                        let (reader, writer) = socket.into_split();

                        match AsyncMysqlIntermediary::run_on(handler, reader, writer).await {
                            Ok(()) => {
                                info!(%peer_addr, "Client disconnected cleanly");
                            }
                            Err(e) => {
                                error!(%peer_addr, error = %e, "Connection error");
                            }
                        }
                    });
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("Received shutdown signal, stopping...");
                    break;
                }
            }
        }

        // Give active connections a moment to finish
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        info!("Proxy stopped.");
        Ok(())
    }
}
