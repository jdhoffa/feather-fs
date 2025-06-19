use clap::Parser;
use quiverfs_core::common_schema::{
    ArrowSchemaInspector, ParquetSchemaInspector, SchemaInspectable,
};
use quiverfs_core::file_discovery::discover_data_files;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "featherfs")]
#[command(about = "Inspect Arrow and Parquet file schemas in a directory", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Inspect all Arrow/Parquet files in a directory
    Inspect {
        /// Directory to scan
        dir: PathBuf,
    },
}

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Inspect { dir } => {
            let files = discover_data_files(dir);
            if files.is_empty() {
                eprintln!("No Arrow or Parquet files found in {:?}", dir);
                std::process::exit(1);
            }
            for file in files {
                let path = file.to_string_lossy();
                let res = if path.ends_with(".arrow") {
                    ArrowSchemaInspector::inspect_schema(&file).map(|schema| format!("{schema:#?}"))
                } else if path.ends_with(".parquet") {
                    ParquetSchemaInspector::inspect_schema(&file)
                        .map(|schema| format!("{schema:#?}"))
                } else {
                    continue;
                };
                match res {
                    Ok(schema) => {
                        println!("File: {path}\nSchema:\n{schema}\n");
                    }
                    Err(e) => {
                        eprintln!("File: {path}\nError: {e}\n");
                    }
                }
            }
        }
    }
}
