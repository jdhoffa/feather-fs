# quiver-fs
A virtual filesystem that mounts a directory of Arrow or Parquet files as a structured set of virtual tables.

## Requirements

- Rust 1.70 or later
- Arrow and Parquet files in a directory

## Installation

Clone the repository and build the CLI using Cargo:

```sh
git clone https://github.com/yourusername/quiver-fs.git
cd quiver-fs
cargo build --release -p quiverfs-cli
```

The compiled binary will be located at `target/release/quiverfs-cli`.

## Usage

To inspect all Arrow and Parquet files in a directory, run: 

``` sh
./target/release/quiverfs-cli inspect /path/to/directory

## OR

cargo run --release --bin quiverfs-cli inspect /path/to/directory
```

You will see output like:

```
File: /path/to/your/data/example.parquet
Schema:
TableSchema {
    fields: [
        TableField {
            name: "id",
            data_type: "INT32",
            nullable: true,
        },
        TableField {
            name: "name",
            data_type: "BYTE_ARRAY",
            nullable: true,
        },
    ],
    format: Parquet,
}

File: /path/to/your/data/example.arrow
Schema:
TableSchema {
    fields: [
        TableField {
            name: "id",
            data_type: "Int32",
            nullable: true,
        },
        TableField {
            name: "name",
            data_type: "Utf8",
            nullable: true,
        },
    ],
    format: Arrow,
}
```

If no valid Arrow or Parquet files are found, you will see:
``` sh
No Arrow or Parquet files found in "directory"
```
