use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};

/// Resolve the active overlay directory from a base path.
///
/// If `.active` exists, read it and return `<base>/<name>/`.
/// If the base already contains `.db` files directly (old format), return `base` as-is.
/// Otherwise default to `<base>/default/`.
pub fn resolve_overlay_dir(base: &Path) -> PathBuf {
    let active_file = base.join(".active");

    // Backward compat: if there are .db files directly in the base, treat it as-is.
    if !active_file.exists() && has_db_files(base) {
        return base.to_path_buf();
    }

    let name = std::fs::read_to_string(&active_file)
        .unwrap_or_else(|_| "default".to_string())
        .trim()
        .to_string();

    let name = if name.is_empty() {
        "default".to_string()
    } else {
        name
    };

    base.join(&name)
}

fn has_db_files(dir: &Path) -> bool {
    if !dir.exists() {
        return false;
    }
    std::fs::read_dir(dir)
        .ok()
        .map(|mut entries| {
            entries.any(|e| {
                e.ok()
                    .and_then(|e| {
                        e.path()
                            .extension()
                            .and_then(|x| x.to_str())
                            .map(|x| x == "db")
                    })
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

/// Validate an overlay name: only alphanumerics, hyphens, underscores.
fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() {
        bail!("Overlay name must not be empty");
    }
    if !name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
    {
        bail!(
            "Overlay name '{}' contains invalid characters (use alphanumerics, hyphens, underscores)",
            name
        );
    }
    Ok(())
}

/// Read the active overlay name from `.active`, defaulting to "default".
fn read_active(base: &Path) -> String {
    let active_file = base.join(".active");
    std::fs::read_to_string(&active_file)
        .unwrap_or_else(|_| "default".to_string())
        .trim()
        .to_string()
}

/// Write the active overlay name to `.active`.
fn write_active(base: &Path, name: &str) -> Result<()> {
    std::fs::create_dir_all(base)
        .with_context(|| format!("Failed to create overlay base directory: {}", base.display()))?;
    std::fs::write(base.join(".active"), name)
        .with_context(|| format!("Failed to write .active file in: {}", base.display()))
}

/// `overlay create <NAME>` — create a new empty overlay subdirectory.
pub fn create_overlay(base: &Path, name: &str) -> Result<()> {
    validate_name(name)?;

    let overlay_dir = base.join(name);
    if overlay_dir.exists() {
        bail!("Overlay '{}' already exists", name);
    }

    std::fs::create_dir_all(&overlay_dir)
        .with_context(|| format!("Failed to create overlay directory: {}", overlay_dir.display()))?;

    // Ensure .active exists (initialise to "default" if first overlay being created).
    let active_file = base.join(".active");
    if !active_file.exists() {
        write_active(base, "default")?;
    }

    println!("Created overlay '{}'", name);
    Ok(())
}

/// `overlay list` — list all overlays, marking the active one with `*`.
pub fn list_overlays(base: &Path) -> Result<()> {
    if !base.exists() {
        println!("No overlays found (base directory does not exist: {})", base.display());
        return Ok(());
    }

    let active = read_active(base);

    let mut overlays: Vec<String> = Vec::new();
    for entry in std::fs::read_dir(base)
        .with_context(|| format!("Failed to read overlay base: {}", base.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir()
            && let Some(name) = path.file_name().and_then(|n| n.to_str())
        {
            overlays.push(name.to_string());
        }
    }

    overlays.sort();

    if overlays.is_empty() {
        println!("No overlays found in {}", base.display());
    } else {
        for name in &overlays {
            if name == &active {
                println!("* {}", name);
            } else {
                println!("  {}", name);
            }
        }
    }

    Ok(())
}

/// `overlay switch <NAME>` — switch the active overlay.
pub fn switch_overlay(base: &Path, name: &str) -> Result<()> {
    validate_name(name)?;

    let overlay_dir = base.join(name);
    if !overlay_dir.exists() {
        bail!(
            "Overlay '{}' does not exist. Use `overlay create {}` to create it.",
            name, name
        );
    }

    write_active(base, name)?;
    println!("Switched to overlay '{}'. Restart the proxy to apply.", name);
    Ok(())
}

/// `overlay delete <NAME>` — delete an overlay. Errors if it is the active one.
pub fn delete_overlay(base: &Path, name: &str) -> Result<()> {
    validate_name(name)?;

    let active = read_active(base);
    if active == name {
        bail!(
            "Cannot delete the active overlay '{}'. Switch to a different overlay first.",
            name
        );
    }

    let overlay_dir = base.join(name);
    if !overlay_dir.exists() {
        bail!("Overlay '{}' does not exist", name);
    }

    std::fs::remove_dir_all(&overlay_dir)
        .with_context(|| format!("Failed to delete overlay directory: {}", overlay_dir.display()))?;

    println!("Deleted overlay '{}'", name);
    Ok(())
}

/// `overlay active` — print the active overlay name.
pub fn show_active(base: &Path) -> Result<()> {
    let active_file = base.join(".active");
    if !active_file.exists() {
        // Backward compat or uninitialised.
        println!("default (no .active file; using default)");
    } else {
        let name = read_active(base);
        println!("{}", name);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_create_overlay() {
        let base = TempDir::new().unwrap();
        create_overlay(base.path(), "feature-auth").unwrap();
        assert!(base.path().join("feature-auth").exists());
        assert!(base.path().join(".active").exists());
    }

    #[test]
    fn test_create_overlay_duplicate_fails() {
        let base = TempDir::new().unwrap();
        create_overlay(base.path(), "my-overlay").unwrap();
        let err = create_overlay(base.path(), "my-overlay").unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn test_list_overlays() {
        let base = TempDir::new().unwrap();
        create_overlay(base.path(), "alpha").unwrap();
        create_overlay(base.path(), "beta").unwrap();
        // Should not panic.
        list_overlays(base.path()).unwrap();
    }

    #[test]
    fn test_switch_overlay() {
        let base = TempDir::new().unwrap();
        create_overlay(base.path(), "default").unwrap();
        create_overlay(base.path(), "feature-payments").unwrap();

        switch_overlay(base.path(), "feature-payments").unwrap();

        let active = read_active(base.path());
        assert_eq!(active, "feature-payments");
    }

    #[test]
    fn test_switch_overlay_nonexistent_fails() {
        let base = TempDir::new().unwrap();
        create_overlay(base.path(), "default").unwrap();
        let err = switch_overlay(base.path(), "nonexistent").unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn test_delete_overlay() {
        let base = TempDir::new().unwrap();
        create_overlay(base.path(), "default").unwrap();
        create_overlay(base.path(), "to-delete").unwrap();

        delete_overlay(base.path(), "to-delete").unwrap();
        assert!(!base.path().join("to-delete").exists());
    }

    #[test]
    fn test_delete_active_overlay_fails() {
        let base = TempDir::new().unwrap();
        create_overlay(base.path(), "default").unwrap();
        // Switch so "default" is active (it is by default after create).
        let err = delete_overlay(base.path(), "default").unwrap_err();
        assert!(err.to_string().contains("Cannot delete the active overlay"));
    }

    #[test]
    fn test_resolve_overlay_dir() {
        let base = TempDir::new().unwrap();
        create_overlay(base.path(), "default").unwrap();
        create_overlay(base.path(), "feature-x").unwrap();
        switch_overlay(base.path(), "feature-x").unwrap();

        let resolved = resolve_overlay_dir(base.path());
        assert_eq!(resolved, base.path().join("feature-x"));
    }

    #[test]
    fn test_resolve_overlay_dir_default() {
        let base = TempDir::new().unwrap();
        create_overlay(base.path(), "default").unwrap();

        let resolved = resolve_overlay_dir(base.path());
        assert_eq!(resolved, base.path().join("default"));
    }

    #[test]
    fn test_backward_compat() {
        // Old format: .db files directly in the base directory (no .active).
        let base = TempDir::new().unwrap();
        std::fs::write(base.path().join("mydb.db"), b"").unwrap();

        let resolved = resolve_overlay_dir(base.path());
        // Should return the base itself, not base/default.
        assert_eq!(resolved, base.path().to_path_buf());
    }
}
