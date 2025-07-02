mod fs;
use fs::QuiverFS;
use fuser::MountOption;
use std::path::PathBuf;

fn main() {
    let source_dir = std::env::args().nth(1).unwrap_or_else(|| ".".to_string());
    let mount_point = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "/tmp/quiver-fs".to_string());

    let filesystem = QuiverFS::new(PathBuf::from(source_dir));

    fuser::mount2(filesystem, &mount_point, &[MountOption::RO]).unwrap();
}
