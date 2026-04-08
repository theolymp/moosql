use anyhow::{Context, Result};
use mysql_async::{Conn, Opts, OptsBuilder};

/// Verify credentials by connecting to the upstream MariaDB server.
/// Returns the established connection on success.
pub async fn verify_upstream(
    upstream_addr: &str,
    user: &str,
    password: &str,
    db: Option<&str>,
) -> Result<Conn> {
    // Parse host:port from upstream_addr
    let (host, port) = if let Some(colon_pos) = upstream_addr.rfind(':') {
        let host = &upstream_addr[..colon_pos];
        let port_str = &upstream_addr[colon_pos + 1..];
        let port: u16 = port_str
            .parse()
            .with_context(|| format!("Invalid port in upstream address: {}", upstream_addr))?;
        (host.to_string(), port)
    } else {
        (upstream_addr.to_string(), 3306u16)
    };

    let mut builder = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port)
        .user(Some(user))
        .pass(Some(password));

    if let Some(db_name) = db {
        builder = builder.db_name(Some(db_name));
    }

    let opts = Opts::from(builder);
    let conn = Conn::new(opts)
        .await
        .with_context(|| format!("Failed to connect to upstream at {}", upstream_addr))?;

    Ok(conn)
}
