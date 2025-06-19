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
            if path.is_dir() {
                visit_dir(&path, files, exts);
            } else if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
                if exts.contains(&ext) && !is_hidden(&path) {
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

        // Create supported files
        let supported = [
            "a.arrow",
            "b.feather",
            "c.parquet",
            "subdir/d.arrow",
            "subdir/.hidden.arrow",
            ".hidden.feather",
            "subdir2/e.txt",
        ];

        for file in &supported {
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

        // Should find only non-hidden supported files
        assert!(found_files.contains(&"a.arrow".to_string()));
        assert!(found_files.contains(&"b.feather".to_string()));
        assert!(found_files.contains(&"c.parquet".to_string()));
        assert!(found_files.contains(&"d.arrow".to_string()));
        assert!(!found_files.contains(&".hidden.arrow".to_string()));
        assert!(!found_files.contains(&".hidden.feather".to_string()));
        assert!(!found_files.contains(&"e.txt".to_string()));
    }

    #[test]
    fn test_is_hidden() {
        assert!(is_hidden(Path::new(".hidden")));
        assert!(is_hidden(Path::new(".hidden.arrow")));
        assert!(!is_hidden(Path::new("visible.arrow")));
    }
}
