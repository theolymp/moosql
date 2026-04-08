use std::path::PathBuf;

use anyhow::Result;
use opensrv_mysql::AsyncMysqlIntermediary;
use tokio::net::TcpListener;
use tracing::{error, info};

use super::handler::CowHandler;

pub struct ProxyServer {
    pub listen_addr: String,
    pub upstream_addr: String,
    pub upstream_user: String,
    pub upstream_password: String,
    pub overlay_dir: PathBuf,
}

impl ProxyServer {
    pub fn new(
        listen_addr: impl Into<String>,
        upstream_addr: impl Into<String>,
        upstream_user: impl Into<String>,
        upstream_password: impl Into<String>,
        overlay_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            listen_addr: listen_addr.into(),
            upstream_addr: upstream_addr.into(),
            upstream_user: upstream_user.into(),
            upstream_password: upstream_password.into(),
            overlay_dir: overlay_dir.into(),
        }
    }

    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.listen_addr).await?;
        info!(
            listen = %self.listen_addr,
            upstream = %self.upstream_addr,
            user = %self.upstream_user,
            "Proxy listening"
        );

        loop {
            let (socket, peer_addr) = listener.accept().await?;
            info!(%peer_addr, "Accepted connection");

            let upstream_addr = self.upstream_addr.clone();
            let upstream_user = self.upstream_user.clone();
            let upstream_password = self.upstream_password.clone();
            let overlay_dir = self.overlay_dir.clone();

            tokio::spawn(async move {
                let handler = CowHandler::new(upstream_addr, upstream_user, upstream_password, overlay_dir);

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
    }
}
