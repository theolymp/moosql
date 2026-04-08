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

    // Third table for multi-table JOINs.
    conn.query_drop(
        "CREATE TABLE IF NOT EXISTS categories (
            id   INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL
        )",
    )
    .await
    .expect("CREATE TABLE categories failed");

    conn.query_drop(
        "INSERT INTO categories (name) VALUES ('Electronics'), ('Accessories')",
    )
    .await
    .expect("INSERT categories failed");

    // Products table with category FK for multi-table JOIN tests.
    conn.query_drop(
        "CREATE TABLE IF NOT EXISTS products (
            id          INT AUTO_INCREMENT PRIMARY KEY,
            name        VARCHAR(100) NOT NULL,
            category_id INT NOT NULL,
            price       DECIMAL(10,2) NOT NULL DEFAULT 0.00
        )",
    )
    .await
    .expect("CREATE TABLE products failed");

    conn.query_drop(
        "INSERT INTO products (name, category_id, price) VALUES
            ('Widget', 1, 9.99),
            ('Gadget', 1, 19.99),
            ('Cable',  2, 4.99)",
    )
    .await
    .expect("INSERT products failed");

    // Employees table for self-join (manager hierarchy) test.
    conn.query_drop(
        "CREATE TABLE IF NOT EXISTS employees (
            id         INT AUTO_INCREMENT PRIMARY KEY,
            name       VARCHAR(100) NOT NULL,
            manager_id INT DEFAULT NULL
        )",
    )
    .await
    .expect("CREATE TABLE employees failed");

    conn.query_drop(
        "INSERT INTO employees (name, manager_id) VALUES
            ('CEO', NULL),
            ('VP', 1),
            ('Engineer', 2),
            ('Intern', 2)",
    )
    .await
    .expect("INSERT employees failed");

    // A stored procedure for SP rewriting tests.
    conn.query_drop(
        "DROP PROCEDURE IF EXISTS get_user_by_id",
    )
    .await
    .expect("DROP PROCEDURE failed");

    conn.query_drop(
        "CREATE PROCEDURE get_user_by_id(IN uid INT)
         BEGIN
             SELECT id, name, email FROM users WHERE id = uid;
         END",
    )
    .await
    .expect("CREATE PROCEDURE failed");
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

// ══════════════════════════════════════════════════════════════════════════════
// Core Proxy Tests
// ══════════════════════════════════════════════════════════════════════════════

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

/// INSERT without specifying all columns -- the omitted column (`active`)
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

// ══════════════════════════════════════════════════════════════════════════════
// Complex Query Tests
// ══════════════════════════════════════════════════════════════════════════════

/// Multi-table JOIN: users + orders + products across 3 tables.
#[tokio::test]
#[ignore]
async fn test_multi_table_join() {
    let fix = Fixture::new().await;

    let proxy = fix.proxy_pool();
    let mut conn = proxy.get_conn().await.unwrap();

    // Dirty products via overlay
    conn.query_drop("UPDATE products SET price = 12.99 WHERE id = 1")
        .await
        .unwrap();

    let rows: Vec<(String, String, String)> = conn
        .query(
            "SELECT u.name, o.product, p.name \
             FROM users u \
             JOIN orders o ON o.user_id = u.id \
             JOIN products p ON p.name = o.product \
             ORDER BY u.id",
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, "Alice");
    assert_eq!(rows[0].1, "Widget");
    assert_eq!(rows[0].2, "Widget");
}

/// Self-join: employee manager hierarchy.
#[tokio::test]
#[ignore]
async fn test_self_join() {
    let fix = Fixture::new().await;

    let proxy = fix.proxy_pool();
    let mut conn = proxy.get_conn().await.unwrap();

    // Overlay-update a manager name
    conn.query_drop("UPDATE employees SET name = 'VicePresident' WHERE id = 2")
        .await
        .unwrap();

    let rows: Vec<(String, String)> = conn
        .query(
            "SELECT e.name, m.name \
             FROM employees e \
             JOIN employees m ON e.manager_id = m.id \
             ORDER BY e.id",
        )
        .await
        .unwrap();

    // VP(2) -> CEO(1), Engineer(3) -> VP(2), Intern(4) -> VP(2)
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], ("VicePresident".to_string(), "CEO".to_string()));
    assert_eq!(rows[1], ("Engineer".to_string(), "VicePresident".to_string()));
    assert_eq!(rows[2], ("Intern".to_string(), "VicePresident".to_string()));
}

/// Subquery: WHERE IN (SELECT ...) referencing overlay data.
#[tokio::test]
#[ignore]
async fn test_subquery() {
    let fix = Fixture::new().await;

    let proxy = fix.proxy_pool();
    let mut conn = proxy.get_conn().await.unwrap();

    // Insert a new user in the overlay
    conn.query_drop(
        "INSERT INTO users (name, email) VALUES ('Charlie', 'charlie@test.com')",
    )
    .await
    .unwrap();

    // Insert an order for Charlie (user_id will be 3)
    conn.query_drop("INSERT INTO orders (user_id, product) VALUES (3, 'Thingamajig')")
        .await
        .unwrap();

    // Subquery: orders for users named 'Charlie'
    let rows: Vec<String> = conn
        .query(
            "SELECT o.product FROM orders o WHERE o.user_id IN \
             (SELECT id FROM users WHERE name = 'Charlie')",
        )
        .await
        .unwrap();

    assert_eq!(rows, vec!["Thingamajig".to_string()]);
}

/// GROUP BY with HAVING on a dirty table.
#[tokio::test]
#[ignore]
async fn test_group_by_having() {
    let fix = Fixture::new().await;

    let proxy = fix.proxy_pool();
    let mut conn = proxy.get_conn().await.unwrap();

    // Insert more products in overlay to create groups
    conn.query_drop(
        "INSERT INTO products (name, category_id, price) VALUES ('Doohickey', 1, 29.99)",
    )
    .await
    .unwrap();

    let rows: Vec<(i32, i64)> = conn
        .query(
            "SELECT category_id, COUNT(*) as cnt FROM products \
             GROUP BY category_id HAVING COUNT(*) >= 3 \
             ORDER BY category_id",
        )
        .await
        .unwrap();

    // Category 1 now has Widget, Gadget, Doohickey = 3
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1, 3));
}

/// UNION combining overlay and upstream data.
#[tokio::test]
#[ignore]
async fn test_union_query() {
    let fix = Fixture::new().await;

    let proxy = fix.proxy_pool();
    let mut conn = proxy.get_conn().await.unwrap();

    // Insert an overlay user
    conn.query_drop(
        "INSERT INTO users (name, email) VALUES ('UnionUser', 'union@test.com')",
    )
    .await
    .unwrap();

    let rows: Vec<(String,)> = conn
        .query(
            "SELECT name FROM users WHERE name = 'Alice' \
             UNION ALL \
             SELECT name FROM users WHERE name = 'UnionUser'",
        )
        .await
        .unwrap();

    let names: Vec<&str> = rows.iter().map(|r| r.0.as_str()).collect();
    assert!(names.contains(&"Alice"));
    assert!(names.contains(&"UnionUser"));
}

/// EXISTS subquery referencing overlay data.
#[tokio::test]
#[ignore]
async fn test_exists_subquery() {
    let fix = Fixture::new().await;

    let proxy = fix.proxy_pool();
    let mut conn = proxy.get_conn().await.unwrap();

    // Delete Bob's order
    conn.query_drop("DELETE FROM orders WHERE user_id = 2")
        .await
        .unwrap();

    // Users who have at least one order (EXISTS)
    let rows: Vec<String> = conn
        .query(
            "SELECT u.name FROM users u WHERE EXISTS \
             (SELECT 1 FROM orders o WHERE o.user_id = u.id) \
             ORDER BY u.id",
        )
        .await
        .unwrap();

    // Only Alice should remain (Bob's order was deleted)
    assert_eq!(rows, vec!["Alice".to_string()]);
}

/// LIMIT and OFFSET on overlay-enriched results.
#[tokio::test]
#[ignore]
async fn test_limit_offset() {
    let fix = Fixture::new().await;

    let proxy = fix.proxy_pool();
    let mut conn = proxy.get_conn().await.unwrap();

    // Insert a third user
    conn.query_drop(
        "INSERT INTO users (name, email) VALUES ('Charlie', 'charlie@test.com')",
    )
    .await
    .unwrap();

    let rows: Vec<String> = conn
        .query("SELECT name FROM users ORDER BY id LIMIT 2 OFFSET 1")
        .await
        .unwrap();

    // Offset 1 means skip Alice, take Bob and Charlie
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], "Bob");
    assert_eq!(rows[1], "Charlie");
}

/// NULL handling: IS NULL, COALESCE with overlay data.
#[tokio::test]
#[ignore]
async fn test_null_handling() {
    let fix = Fixture::new().await;

    let proxy = fix.proxy_pool();
    let mut conn = proxy.get_conn().await.unwrap();

    // Find employees with no manager (NULL manager_id)
    let rows: Vec<String> = conn
        .query(
            "SELECT name FROM employees WHERE manager_id IS NULL",
        )
        .await
        .unwrap();
    assert_eq!(rows, vec!["CEO".to_string()]);

    // COALESCE: replace NULL manager_id with 0
    let rows: Vec<(String, i64)> = conn
        .query(
            "SELECT name, COALESCE(manager_id, 0) FROM employees ORDER BY id LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(rows[0], ("CEO".to_string(), 0));
}

/// CASE expression with overlay data.
#[tokio::test]
#[ignore]
async fn test_case_expression() {
    let fix = Fixture::new().await;

    let proxy = fix.proxy_pool();
    let mut conn = proxy.get_conn().await.unwrap();

    // Insert overlay user with active = 0
    conn.query_drop(
        "INSERT INTO users (name, email, active) VALUES ('Inactive', 'inactive@test.com', 0)",
    )
    .await
    .unwrap();

    let rows: Vec<(String, String)> = conn
        .query(
            "SELECT name, CASE WHEN active = 1 THEN 'yes' ELSE 'no' END as status \
             FROM users ORDER BY id",
        )
        .await
        .unwrap();

    // Last row should be inactive
    let last = rows.last().unwrap();
    assert_eq!(last.0, "Inactive");
    assert_eq!(last.1, "no");
}

/// COUNT includes overlay-inserted rows.
#[tokio::test]
#[ignore]
async fn test_count_with_overlay() {
    let fix = Fixture::new().await;

    let proxy = fix.proxy_pool();
    let mut conn = proxy.get_conn().await.unwrap();

    // Baseline count
    let before: Option<i64> = conn
        .query_first("SELECT COUNT(*) FROM users")
        .await
        .unwrap();

    conn.query_drop(
        "INSERT INTO users (name, email) VALUES ('Counted', 'counted@test.com')",
    )
    .await
    .unwrap();

    let after: Option<i64> = conn
        .query_first("SELECT COUNT(*) FROM users")
        .await
        .unwrap();

    assert_eq!(after.unwrap(), before.unwrap() + 1);
}

// ══════════════════════════════════════════════════════════════════════════════
// DDL Tests
// ══════════════════════════════════════════════════════════════════════════════

/// CREATE TABLE through the proxy should only exist in the overlay.
#[tokio::test]
#[ignore]
async fn test_create_table_overlay() {
    let fix = Fixture::new().await;

    let proxy = fix.proxy_pool();
    let mut conn = proxy.get_conn().await.unwrap();

    conn.query_drop(
        "CREATE TABLE test_overlay_only (
            id   INT AUTO_INCREMENT PRIMARY KEY,
            data VARCHAR(100)
        )",
    )
    .await
    .unwrap();

    // Insert and read back through proxy
    conn.query_drop("INSERT INTO test_overlay_only (data) VALUES ('hello')")
        .await
        .unwrap();

    let rows: Vec<String> = conn
        .query("SELECT data FROM test_overlay_only")
        .await
        .unwrap();
    assert_eq!(rows, vec!["hello".to_string()]);

    // Verify it does NOT exist upstream
    let mut up_conn = fix.upstream.get_conn().await.unwrap();
    let result = up_conn
        .query_drop("SELECT 1 FROM test_overlay_only LIMIT 1")
        .await;
    assert!(result.is_err(), "Table should not exist in upstream");
}

/// TRUNCATE TABLE should hide all base rows through the proxy.
#[tokio::test]
#[ignore]
async fn test_truncate_overlay() {
    let fix = Fixture::new().await;

    let proxy = fix.proxy_pool();
    let mut conn = proxy.get_conn().await.unwrap();

    // Truncate the users table via proxy
    conn.query_drop("TRUNCATE TABLE users").await.unwrap();

    // Proxy should show 0 rows
    let count: Option<i64> = conn
        .query_first("SELECT COUNT(*) FROM users")
        .await
        .unwrap();
    assert_eq!(count, Some(0), "Truncated table should show 0 rows via proxy");

    // Upstream should still have its rows
    let mut up_conn = fix.upstream.get_conn().await.unwrap();
    let up_count: Option<i64> = up_conn
        .query_first("SELECT COUNT(*) FROM users")
        .await
        .unwrap();
    assert!(up_count.unwrap() > 0, "Upstream should still have rows");
}

/// SHOW TABLES should include overlay-created tables.
#[tokio::test]
#[ignore]
async fn test_show_tables_includes_overlay() {
    let fix = Fixture::new().await;

    let proxy = fix.proxy_pool();
    let mut conn = proxy.get_conn().await.unwrap();

    conn.query_drop(
        "CREATE TABLE new_overlay_table (id INT PRIMARY KEY, val TEXT)",
    )
    .await
    .unwrap();

    let tables: Vec<String> = conn.query("SHOW TABLES").await.unwrap();
    assert!(
        tables.iter().any(|t| t == "new_overlay_table"),
        "SHOW TABLES should list overlay-created table, got: {:?}",
        tables
    );
}

// ══════════════════════════════════════════════════════════════════════════════
// Stored Procedure Tests
// ══════════════════════════════════════════════════════════════════════════════

/// CALL to a stored procedure should see overlay data.
#[tokio::test]
#[ignore]
async fn test_stored_procedure_rewriting() {
    let fix = Fixture::new().await;

    let proxy = fix.proxy_pool();
    let mut conn = proxy.get_conn().await.unwrap();

    // Update Alice's name in overlay
    conn.query_drop("UPDATE users SET name = 'Alicia' WHERE id = 1")
        .await
        .unwrap();

    // Call the stored procedure -- it should see the overlay data
    let rows: Vec<(i32, String, String)> = conn
        .query("CALL get_user_by_id(1)")
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, "Alicia", "SP should see overlay-updated name");
}
