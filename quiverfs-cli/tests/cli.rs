use assert_cmd::Command;
use std::fs::File;
use std::io::Write;
use tempfile::tempdir;

#[test]
fn test_malformed_parquet_file() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("bad.parquet");
    let mut file = File::create(&file_path).unwrap();
    writeln!(file, "not a real parquet file").unwrap();

    let mut cmd = Command::cargo_bin("featherfs").unwrap();
    cmd.arg("inspect").arg(dir.path());
    cmd.assert()
        .stderr(predicates::str::contains("Error"))
        .stderr(predicates::str::contains("bad.parquet"));
}

#[test]
fn test_malformed_arrow_file() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("bad.arrow");
    let mut file = File::create(&file_path).unwrap();
    writeln!(file, "not a real arrow file").unwrap();

    let mut cmd = Command::cargo_bin("featherfs").unwrap();
    cmd.arg("inspect").arg(dir.path());
    cmd.assert()
        .stderr(predicates::str::contains("Error"))
        .stderr(predicates::str::contains("bad.arrow"));
}
