use std::fs;
use std::path::{Path, PathBuf};

/// Checks if a file is hidden (starts with a dot).
fn is_hidden(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.starts_with('.'))
        .unwrap_or(false)
}

/// Recursively visits directories and collects supported files.
fn visit_dir(dir: &Path, files: &mut Vec<PathBuf>, exts: &[&str]) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() && !is_hidden(&path) {
                visit_dir(&path, files, exts);
            } else if is_hidden(&path) {
                continue; // Skip hidden files
            } else if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
                if exts.contains(&ext) {
                    if let Ok(abs_path) = path.canonicalize() {
                        files.push(abs_path);
                    }
                }
            }
        }
    }
}

/// Recursively scans a directory for .arrow, .feather, or .parquet files and returns their absolute paths.
pub fn discover_data_files<P: AsRef<Path>>(root: P) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let supported_exts = ["arrow", "feather", "parquet"];
    let root = root.as_ref();
    if root.is_dir() {
        visit_dir(root, &mut files, &supported_exts);
    }
    files
}

#[cfg(test)]
mod tests {
    use super::discover_data_files;
    use super::is_hidden;
    use std::fs::{self, File};
    use std::io::Write;
    use std::path::Path;

    #[test]
    fn test_discover_data_files() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let root = tmp_dir.path();

        // Visible files and dirs
        let visible_files = ["a.arrow", "b.feather", "c.parquet", "subdir/d.arrow"];

        // Hidden files and dirs
        let hidden_files = [".hidden.arrow", "subdir/.hidden2.feather", ".tmp.parquet"];
        let hidden_dirs = [
            ".venv/e.arrow",
            ".hidden_dir/f.feather",
            ".cache/data.parquet",
        ];

        for file in &visible_files {
            let file_path = root.join(file);
            if let Some(parent) = file_path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            File::create(&file_path)
                .unwrap()
                .write_all(b"test")
                .unwrap();
        }

        // Create hidden files
        for file in &hidden_files {
            let file_path = root.join(file);
            if let Some(parent) = file_path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            File::create(&file_path)
                .unwrap()
                .write_all(b"test")
                .unwrap();
        }

        // Create hidden directories and files within them
        for file in &hidden_dirs {
            let file_path = root.join(file);
            if let Some(parent) = file_path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            File::create(&file_path)
                .unwrap()
                .write_all(b"test")
                .unwrap();
        }

        let found = discover_data_files(root);
        let found_files: Vec<_> = found
            .iter()
            .filter_map(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|s| s.to_string())
            })
            .collect();

        // Should find only visible files in visible directories
        for file in &["a.arrow", "b.feather", "c.parquet", "d.arrow"] {
            assert!(found_files.contains(&file.to_string()), &format!("Missing {}", file));
        }

        // Should NOT find hidden files or files in hidden directories
        for file in &[
            ".hidden.arrow",
            ".hidden2.feather",
            ".tmp.parquet",
            "e.arrow",
            "f.feather",
            "data.parquet",
        ] {
            assert!(
                !found_files.contains(&file.to_string()),
                "Should not find hidden or nested file: {file}"
            );
        }
    }

    #[test]
    fn test_is_hidden() {
        assert!(is_hidden(Path::new(".hidden")));
        assert!(is_hidden(Path::new(".hidden.arrow")));
        assert!(!is_hidden(Path::new("visible.arrow")));
    }
}
