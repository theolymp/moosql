use anyhow::{bail, Context, Result};
use std::path::Path;

pub fn save_snapshot(overlay_dir: &Path, name: &str, force: bool) -> Result<()> {
    let snapshots_dir = overlay_dir.join(".snapshots");
    let snapshot_dir = snapshots_dir.join(name);

    if snapshot_dir.exists() {
        if force {
            std::fs::remove_dir_all(&snapshot_dir)
                .with_context(|| format!("Failed to remove existing snapshot '{name}'"))?;
        } else {
            bail!("Snapshot '{name}' already exists. Use --force to overwrite.");
        }
    }

    std::fs::create_dir_all(&snapshot_dir)
        .with_context(|| format!("Failed to create snapshot directory for '{name}'"))?;

    // If overlay dir doesn't exist yet, just save empty snapshot
    if !overlay_dir.exists() {
        println!("Snapshot '{name}' saved (0 databases, 0 KB)");
        return Ok(());
    }

    let mut db_count = 0usize;
    let mut total_bytes = 0u64;

    for entry in std::fs::read_dir(overlay_dir)
        .with_context(|| format!("Failed to read overlay directory: {}", overlay_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();

        if path.extension().and_then(|e| e.to_str()) != Some("db") {
            continue;
        }

        let file_name = match path.file_name() {
            Some(n) => n.to_owned(),
            None => continue,
        };

        let dest = snapshot_dir.join(&file_name);
        std::fs::copy(&path, &dest)
            .with_context(|| format!("Failed to copy {:?} to snapshot", file_name))?;

        total_bytes += std::fs::metadata(&dest)
            .map(|m| m.len())
            .unwrap_or(0);
        db_count += 1;
    }

    let kb = (total_bytes + 512) / 1024;
    println!("Snapshot '{name}' saved ({db_count} databases, {kb} KB)");
    Ok(())
}

pub fn restore_snapshot(overlay_dir: &Path, name: &str) -> Result<()> {
    let snapshot_dir = overlay_dir.join(".snapshots").join(name);

    if !snapshot_dir.exists() {
        bail!("Snapshot '{name}' not found");
    }

    // Ensure overlay dir exists
    if !overlay_dir.exists() {
        std::fs::create_dir_all(overlay_dir)
            .with_context(|| format!("Failed to create overlay directory: {}", overlay_dir.display()))?;
    }

    // Delete all .db files in overlay dir (leave .snapshots subdir intact)
    for entry in std::fs::read_dir(overlay_dir)
        .with_context(|| format!("Failed to read overlay directory: {}", overlay_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("db") {
            std::fs::remove_file(&path)
                .with_context(|| format!("Failed to remove {}", path.display()))?;
        }
    }

    // Copy .db files from snapshot dir to overlay dir
    for entry in std::fs::read_dir(&snapshot_dir)
        .with_context(|| format!("Failed to read snapshot directory for '{name}'"))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("db") {
            continue;
        }
        let file_name = match path.file_name() {
            Some(n) => n.to_owned(),
            None => continue,
        };
        let dest = overlay_dir.join(&file_name);
        std::fs::copy(&path, &dest)
            .with_context(|| format!("Failed to restore {:?} from snapshot", file_name))?;
    }

    println!("Snapshot '{name}' restored");
    Ok(())
}

pub fn list_snapshots(overlay_dir: &Path) -> Result<()> {
    let snapshots_dir = overlay_dir.join(".snapshots");

    if !snapshots_dir.exists() {
        println!("No snapshots found.");
        return Ok(());
    }

    let mut entries: Vec<(String, std::time::SystemTime, u64)> = Vec::new();

    for entry in std::fs::read_dir(&snapshots_dir)
        .with_context(|| "Failed to read .snapshots directory")?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n.to_owned(),
            None => continue,
        };

        let modified = std::fs::metadata(&path)
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);

        // Sum up .db file sizes
        let total_bytes: u64 = std::fs::read_dir(&path)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("db"))
                    .filter_map(|e| std::fs::metadata(e.path()).ok())
                    .map(|m| m.len())
                    .sum()
            })
            .unwrap_or(0);

        entries.push((name, modified, total_bytes));
    }

    if entries.is_empty() {
        println!("No snapshots found.");
        return Ok(());
    }

    // Sort by modified time, newest first
    entries.sort_by(|a, b| b.1.cmp(&a.1));

    let name_w = entries.iter().map(|(n, _, _)| n.len()).max().unwrap_or(4).max(4);

    println!(
        "{:<name_w$}  {:<20}  {}",
        "NAME",
        "DATE",
        "SIZE",
        name_w = name_w,
    );
    println!("{}", "-".repeat(name_w + 30));

    for (name, modified, bytes) in &entries {
        let kb = (bytes + 512) / 1024;
        let datetime = humanize_time(*modified);
        println!(
            "{:<name_w$}  {:<20}  {} KB",
            name,
            datetime,
            kb,
            name_w = name_w,
        );
    }

    Ok(())
}

fn humanize_time(t: std::time::SystemTime) -> String {
    let secs = t
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    // Format as YYYY-MM-DD HH:MM:SS UTC using manual arithmetic
    let s = secs % 60;
    let m = (secs / 60) % 60;
    let h = (secs / 3600) % 24;
    let days = secs / 86400;

    // Days since epoch → calendar date (Gregorian proleptic)
    let (year, month, day) = days_to_ymd(days);

    format!("{year:04}-{month:02}-{day:02} {h:02}:{m:02}:{s:02}")
}

fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    // Algorithm from https://howardhinnant.github.io/date_algorithms.html
    let z = days + 719468;
    let era = z / 146097;
    let doe = z % 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn write_dummy_db(dir: &Path, name: &str) {
        let path = dir.join(name);
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(b"SQLite format 3\x00dummy").unwrap();
    }

    #[test]
    fn test_save_and_restore_snapshot() {
        let dir = TempDir::new().unwrap();
        let overlay = dir.path();
        write_dummy_db(overlay, "mydb.db");

        save_snapshot(overlay, "v1", false).unwrap();

        // Verify snapshot exists
        assert!(overlay.join(".snapshots/v1/mydb.db").exists());

        // Overwrite overlay db with different content
        std::fs::write(overlay.join("mydb.db"), b"changed").unwrap();

        // Restore
        restore_snapshot(overlay, "v1").unwrap();

        // Verify content restored
        let content = std::fs::read(overlay.join("mydb.db")).unwrap();
        assert_eq!(&content, b"SQLite format 3\x00dummy");
    }

    #[test]
    fn test_list_snapshots() {
        let dir = TempDir::new().unwrap();
        let overlay = dir.path();
        write_dummy_db(overlay, "mydb.db");

        save_snapshot(overlay, "alpha", false).unwrap();
        save_snapshot(overlay, "beta", false).unwrap();

        // Should not error
        list_snapshots(overlay).unwrap();

        // Both dirs exist
        assert!(overlay.join(".snapshots/alpha").is_dir());
        assert!(overlay.join(".snapshots/beta").is_dir());
    }

    #[test]
    fn test_snapshot_already_exists() {
        let dir = TempDir::new().unwrap();
        let overlay = dir.path();
        write_dummy_db(overlay, "mydb.db");

        save_snapshot(overlay, "snap", false).unwrap();

        let err = save_snapshot(overlay, "snap", false).unwrap_err();
        assert!(err.to_string().contains("already exists"));

        // --force should succeed
        save_snapshot(overlay, "snap", true).unwrap();
    }

    #[test]
    fn test_restore_nonexistent() {
        let dir = TempDir::new().unwrap();
        let overlay = dir.path();

        let err = restore_snapshot(overlay, "ghost").unwrap_err();
        assert!(err.to_string().contains("not found"));
    }
}
