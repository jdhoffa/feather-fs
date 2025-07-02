use fuser::{FileAttr, FileType, Filesystem, ReplyAttr, ReplyDirectory, ReplyEntry, Request};
use std::ffi::OsStr;
use std::path::PathBuf;
use std::time::{Duration, UNIX_EPOCH};
use walkdir::WalkDir;

const TTL: Duration = Duration::from_secs(1);

pub struct QuiverFS {
    source_dir: PathBuf,
    entries: Vec<(u64, String)>, // (inode, name)
}

impl QuiverFS {
    pub fn new(source_dir: PathBuf) -> Self {
        let mut fs = QuiverFS {
            source_dir,
            entries: Vec::new(),
        };
        fs.scan_directory();
        fs
    }

    fn scan_directory(&mut self) {
        let mut inode = 2; // Start at 2 since 1 is root

        for entry in WalkDir::new(&self.source_dir)
            .min_depth(1)
            .max_depth(1)
            .into_iter()
            .filter_map(Result::ok)
        {
            if let Some(ext) = entry.path().extension().and_then(OsStr::to_str) {
                if ["arrow", "parquet", "feather"].contains(&ext.to_lowercase().as_str()) {
                    if let Some(name) = entry.path().file_stem().and_then(OsStr::to_str) {
                        self.entries.push((inode, name.to_string()));
                        inode += 1;
                    }
                }
            }
        }
    }

    fn get_entry_by_name(&self, name: &str) -> Option<(u64, &str)> {
        self.entries
            .iter()
            .find(|(_, entry_name)| entry_name == name)
            .map(|(inode, name)| (*inode, name.as_str()))
    }
}

impl Filesystem for QuiverFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if parent != 1 {
            reply.error(libc::ENOENT);
            return;
        }

        if let Some(name_str) = name.to_str() {
            if let Some((inode, _)) = self.get_entry_by_name(name_str) {
                let attr = FileAttr {
                    ino: inode,
                    size: 0,
                    blocks: 0,
                    atime: UNIX_EPOCH,
                    mtime: UNIX_EPOCH,
                    ctime: UNIX_EPOCH,
                    crtime: UNIX_EPOCH,
                    kind: FileType::Directory,
                    perm: 0o755,
                    nlink: 2,
                    uid: 501,
                    gid: 20,
                    rdev: 0,
                    flags: 0,
                    blksize: 512,
                };
                reply.entry(&TTL, &attr, 0);
                return;
            }
        }
        reply.error(libc::ENOENT);
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        let attr = if ino == 1 {
            // Root directory
            FileAttr {
                ino: 1,
                size: 0,
                blocks: 0,
                atime: UNIX_EPOCH,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                crtime: UNIX_EPOCH,
                kind: FileType::Directory,
                perm: 0o755,
                nlink: 2,
                uid: 501,
                gid: 20,
                rdev: 0,
                flags: 0,
                blksize: 512,
            }
        } else if self.entries.iter().any(|(entry_ino, _)| *entry_ino == ino) {
            // Table directory
            FileAttr {
                ino,
                size: 0,
                blocks: 0,
                atime: UNIX_EPOCH,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                crtime: UNIX_EPOCH,
                kind: FileType::Directory,
                perm: 0o755,
                nlink: 2,
                uid: 501,
                gid: 20,
                rdev: 0,
                flags: 0,
                blksize: 512,
            }
        } else {
            reply.error(libc::ENOENT);
            return;
        };
        reply.attr(&TTL, &attr);
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if ino != 1 {
            reply.error(libc::ENOENT);
            return;
        }

        let mut entries = vec![
            (1, FileType::Directory, "."),
            (1, FileType::Directory, ".."),
        ];

        // Add all discovered tables as directories
        entries.extend(
            self.entries
                .iter()
                .map(|(inode, name)| (*inode, FileType::Directory, name.as_str())),
        );

        for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(entry.0, (i + 1) as i64, entry.1, entry.2) {
                break;
            }
        }
        reply.ok();
    }
}
