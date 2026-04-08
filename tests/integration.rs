// Integration tests for mariadb-cow proxy
//
// These tests require Docker to be installed and running.
// They are marked #[ignore] by default so they don't run on `cargo test`.
// Run with: cargo test -- --ignored
// Or a single test: cargo test test_passthrough_select -- --ignored
//
// Before running, ensure the proxy binary is built:
//   cargo build

use mysql_async::prelude::*;
use std::process::Command;
use std::time::Duration;
use tokio::time::sleep;

const UPSTREAM_PORT: u16 = 23306; // non-standard to avoid conflicts
const PROXY_PORT: u16 = 23307;
const CONTAINER_NAME: &str = "mariadb-cow-integration-test";

// ── helpers ──────────────────────────────────────────────────────────────────

fn upstream_url() -> String {
    format!(
        "mysql://root:testpass@127.0.0.1:{}/testdb",
        UPSTREAM_PORT
    )
}

fn proxy_url() -> String {
    format!(
        "mysql://root:testpass@127.0.0.1:{}/testdb",
        PROXY_PORT
    )
}

/// Start the MariaDB Docker container and wait until it accepts connections.
/// Returns a pool connected directly to the upstream (bypasses the proxy).
async fn start_mariadb() -> mysql_async::Pool {
    // Remove any stale container from a previous run first.
    let _ = Command::new("docker")
        .args(["rm", "-f", CONTAINER_NAME])
        .output();

    let out = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            CONTAINER_NAME,
            "-p",
            &format!("{}:3306", UPSTREAM_PORT),
            "-e",
            "MYSQL_ROOT_PASSWORD=testpass",
            "-e",
            "MYSQL_DATABASE=testdb",
            "mariadb:11",
        ])
        .output()
        .expect("failed to run docker");

    if !out.status.success() {
        panic!(
            "docker run failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    // Poll until the upstream accepts a MySQL connection (up to 60 s).
    let url = upstream_url();
    for attempt in 0..60 {
        match mysql_async::Conn::from_url(&url).await {
            Ok(conn) => {
                drop(conn);
                eprintln!("MariaDB ready after {}s", attempt);
                break;
            }
            Err(_) => sleep(Duration::from_secs(1)).await,
        }
    }

    mysql_async::Pool::new(url.as_str())
}

/// Create the test tables and seed data via a direct upstream connection.
async fn seed_database(pool: &mysql_async::Pool) {
    let mut conn = pool
        .get_conn()
        .await
        .expect("could not connect to upstream");

    conn.query_drop(
        "CREATE TABLE IF NOT EXISTS users (
            id     INT AUTO_INCREMENT PRIMARY KEY,
            name   VARCHAR(100)  NOT NULL,
            email  VARCHAR(200)  NOT NULL,
            active BOOLEAN DEFAULT 1
        )",
    )
    .await
    .expect("CREATE TABLE users failed");

    conn.query_drop(
        "INSERT INTO users (name, email) VALUES
            ('Alice', 'alice@test.com'),
            ('Bob',   'bob@test.com')",
    )
    .await
    .expect("INSERT seed rows failed");

    // Second table used by the JOIN test.
    conn.query_drop(
        "CREATE TABLE IF NOT EXISTS orders (
            id      INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            product VARCHAR(100) NOT NULL
        )",
    )
    .await
    .expect("CREATE TABLE orders failed");

    conn.query_drop(
        "INSERT INTO orders (user_id, product) VALUES (1, 'Widget'), (2, 'Gadget')",
    )
    .await
    .expect("INSERT orders failed");
}

/// Kill the Docker container.
fn teardown() {
    let _ = Command::new("docker")
        .args(["rm", "-f", CONTAINER_NAME])
        .output();
}

/// Spawn the proxy binary pointing at the upstream container, using a
/// caller-supplied overlay directory.  The process is killed when the
/// returned `Child` is dropped (`kill_on_drop(true)`).
async fn start_proxy(overlay_dir: &std::path::Path) -> tokio::process::Child {
    // Prefer the pre-built debug binary; fall back to `cargo run` so the
    // test can be invoked without a prior `cargo build`.
    let binary = std::path::Path::new("./target/debug/mariadb-cow");

    let mut cmd = if binary.exists() {
        let mut c = tokio::process::Command::new(binary);
        c.args([
            "start",
            &format!("--upstream=127.0.0.1:{}", UPSTREAM_PORT),
            &format!("--listen=127.0.0.1:{}", PROXY_PORT),
            "--user=root",
            "--password=testpass",
            &format!("--overlay={}", overlay_dir.display()),
        ]);
        c
    } else {
        let mut c = tokio::process::Command::new("cargo");
        c.args([
            "run",
            "--",
            "start",
            &format!("--upstream=127.0.0.1:{}", UPSTREAM_PORT),
            &format!("--listen=127.0.0.1:{}", PROXY_PORT),
            "--user=root",
            "--password=testpass",
            &format!("--overlay={}", overlay_dir.display()),
        ]);
        c
    };

    cmd.kill_on_drop(true);
    // Suppress noisy proxy logs in test output; set RUST_LOG=info to re-enable.
    if std::env::var("RUST_LOG").is_err() {
        cmd.env("RUST_LOG", "error");
    }

    let child = cmd.spawn().expect("failed to spawn proxy process");

    // Poll the proxy port until it starts accepting connections (up to 30 s).
    let url = proxy_url();
    for attempt in 0..30 {
        match mysql_async::Conn::from_url(&url).await {
            Ok(conn) => {
                drop(conn);
                eprintln!("Proxy ready after {}s", attempt);
                return child;
            }
            Err(_) => sleep(Duration::from_secs(1)).await,
        }
    }

    panic!("Proxy did not become ready within 30 s");
}

// ── full test fixture ─────────────────────────────────────────────────────────

/// Everything a single test needs: upstream pool, proxy child process,
/// and a temp directory that owns the overlay data.
struct Fixture {
    upstream: mysql_async::Pool,
    _proxy: tokio::process::Child,
    _overlay_dir: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> Self {
        let upstream = start_mariadb().await;
        seed_database(&upstream).await;

        let overlay_dir = tempfile::tempdir().expect("could not create tempdir");
        let proxy = start_proxy(overlay_dir.path()).await;

        Fixture {
            upstream,
            _proxy: proxy,
            _overlay_dir: overlay_dir,
        }
    }

    fn proxy_pool(&self) -> mysql_async::Pool {
        mysql_async::Pool::new(proxy_url().as_str())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        teardown();
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// SELECT through the proxy returns the same rows as a direct query.
#[tokio::test]
#[ignore]
async fn test_passthrough_select() {
    let fix = Fixture::new().await;

    let proxy = fix.proxy_pool();
    let mut conn = proxy.get_conn().await.unwrap();

    let rows: Vec<(u32, String, String)> = conn
        .query("SELECT id, name, email FROM users ORDER BY id")
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, "Alice".into(), "alice@test.com".into()));
    assert_eq!(rows[1], (2, "Bob".into(), "bob@test.com".into()));
}

/// INSERT through the proxy is visible via the proxy but NOT in the upstream.
#[tokio::test]
#[ignore]
async fn test_insert_overlay() {
    let fix = Fixture::new().await;

    // Insert through the proxy.
    let proxy = fix.proxy_pool();
    let mut proxy_conn = proxy.get_conn().await.unwrap();
    proxy_conn
        .query_drop(
            "INSERT INTO users (name, email) VALUES ('Charlie', 'charlie@test.com')",
        )
        .await
        .unwrap();

    // Row must be visible through the proxy.
    let proxy_rows: Vec<String> = proxy_conn
        .query("SELECT name FROM users WHERE name = 'Charlie'")
        .await
        .unwrap();
    assert_eq!(proxy_rows, vec!["Charlie".to_string()]);

    // Row must NOT appear in the upstream (CoW isolation).
    let mut up_conn = fix.upstream.get_conn().await.unwrap();
    let up_rows: Vec<String> = up_conn
        .query("SELECT name FROM users WHERE name = 'Charlie'")
        .await
        .unwrap();
    assert!(up_rows.is_empty(), "Charlie should not exist in upstream");
}

/// UPDATE through the proxy is reflected via the proxy but the upstream row
/// keeps its original value.
#[tokio::test]
#[ignore]
async fn test_update_overlay() {
    let fix = Fixture::new().await;

    let proxy = fix.proxy_pool();
    let mut proxy_conn = proxy.get_conn().await.unwrap();
    proxy_conn
        .query_drop("UPDATE users SET email = 'alice-new@test.com' WHERE id = 1")
        .await
        .unwrap();

    // Proxy should see the updated email.
    let proxy_email: Option<String> = proxy_conn
        .query_first("SELECT email FROM users WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(proxy_email.as_deref(), Some("alice-new@test.com"));

    // Upstream should still have the original email.
    let mut up_conn = fix.upstream.get_conn().await.unwrap();
    let up_email: Option<String> = up_conn
        .query_first("SELECT email FROM users WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(up_email.as_deref(), Some("alice@test.com"));
}

/// DELETE through the proxy hides the row via the proxy but the upstream row
/// is untouched.
#[tokio::test]
#[ignore]
async fn test_delete_overlay() {
    let fix = Fixture::new().await;

    let proxy = fix.proxy_pool();
    let mut proxy_conn = proxy.get_conn().await.unwrap();
    proxy_conn
        .query_drop("DELETE FROM users WHERE id = 2")
        .await
        .unwrap();

    // Row must be invisible through the proxy.
    let proxy_rows: Vec<String> = proxy_conn
        .query("SELECT name FROM users WHERE id = 2")
        .await
        .unwrap();
    assert!(proxy_rows.is_empty(), "Bob should be hidden via proxy");

    // Row must still exist in the upstream.
    let mut up_conn = fix.upstream.get_conn().await.unwrap();
    let up_name: Option<String> = up_conn
        .query_first("SELECT name FROM users WHERE id = 2")
        .await
        .unwrap();
    assert_eq!(up_name.as_deref(), Some("Bob"));
}

/// INSERT without specifying all columns — the omitted column (`active`)
/// should receive its DEFAULT value and be returned correctly.
#[tokio::test]
#[ignore]
async fn test_default_values() {
    let fix = Fixture::new().await;

    let proxy = fix.proxy_pool();
    let mut conn = proxy.get_conn().await.unwrap();

    conn.query_drop(
        "INSERT INTO users (name, email) VALUES ('DefaultUser', 'default@test.com')",
    )
    .await
    .unwrap();

    // The `active` column should default to 1 (TRUE).
    let active: Option<bool> = conn
        .query_first("SELECT active FROM users WHERE name = 'DefaultUser'")
        .await
        .unwrap();

    assert_eq!(active, Some(true), "active should default to 1 / TRUE");
}

/// LAST_INSERT_ID() after an overlay INSERT must return the new row's id.
#[tokio::test]
#[ignore]
async fn test_last_insert_id() {
    let fix = Fixture::new().await;

    let proxy = fix.proxy_pool();
    let mut conn = proxy.get_conn().await.unwrap();

    conn.query_drop(
        "INSERT INTO users (name, email) VALUES ('LIIDUser', 'liid@test.com')",
    )
    .await
    .unwrap();

    let last_id: Option<u64> = conn
        .query_first("SELECT LAST_INSERT_ID()")
        .await
        .unwrap();

    assert!(
        last_id.unwrap_or(0) > 0,
        "LAST_INSERT_ID() should be a positive integer after INSERT"
    );
}

/// A JOIN that spans a table with overlay writes and a clean table must
/// return correct combined results.
#[tokio::test]
#[ignore]
async fn test_join_with_dirty() {
    let fix = Fixture::new().await;

    // Dirty the `users` table via the proxy.
    let proxy = fix.proxy_pool();
    let mut conn = proxy.get_conn().await.unwrap();
    conn.query_drop(
        "UPDATE users SET name = 'Alicia' WHERE id = 1",
    )
    .await
    .unwrap();

    // JOIN users (dirty) with orders (clean).
    let rows: Vec<(String, String)> = conn
        .query(
            "SELECT u.name, o.product \
             FROM users u \
             JOIN orders o ON o.user_id = u.id \
             ORDER BY u.id",
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    // Row 1: updated name from overlay, order from upstream.
    assert_eq!(rows[0], ("Alicia".to_string(), "Widget".to_string()));
    // Row 2: unmodified name, order from upstream.
    assert_eq!(rows[1], ("Bob".to_string(), "Gadget".to_string()));
}
