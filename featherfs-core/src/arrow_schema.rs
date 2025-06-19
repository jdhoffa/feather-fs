use arrow::ipc::reader::FileReader;
use serde_json::json;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

pub fn arrow_schema_to_json<P: AsRef<Path>>(path: P) -> Result<serde_json::Value, String> {
    let file = File::open(&path).map_err(|e| format!("Failed to open file: {}", e))?;
    let reader = FileReader::try_new(BufReader::new(file), None)
        .map_err(|e| format!("Failed to read Arrow IPC file: {}", e))?;

    let schema = reader.schema();
    // Convert Arrow Schema to JSON
    let fields_json: Vec<_> = schema
        .fields()
        .iter()
        .map(|f| {
            json!({
                "name": f.name(),
                "data_type": format!("{:?}", f.data_type()),
                "nullable": f.is_nullable(),
                "metadata": f.metadata(),
            })
        })
        .collect();

    Ok(json!({
        "fields": fields_json,
        "metadata": schema.metadata(),
    }))
}
