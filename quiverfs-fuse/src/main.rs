use fuser::MountOption;
mod fs;

fn main() {
    let mountpoint = std::env::args()
    .nth(1)
    .unwrap_or_else(|| "/tmp/quiver-fs".to_string());

    let filesystem = fs::hello_fs::HelloFS::new();

    fuser::mount2(filesystem, &mountpoint, &[MountOption::RO]).unwrap();
}