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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use arrow::ipc::writer::FileWriter;
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    #[test]
    fn test_arrow_schema_to_json_valid_file() {
        // Create a temporary Arrow IPC file with a known schema
        let file = NamedTempFile::new().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", arrow::datatypes::DataType::Int32, false),
            Field::new("name", arrow::datatypes::DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap();

        {
            let mut writer = FileWriter::try_new(file.reopen().unwrap(), &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        // Test the function
        let result = arrow_schema_to_json(file.path());
        assert!(result.is_ok());
        let json = result.unwrap();
        assert_eq!(json["fields"][0]["name"], "id");
        assert_eq!(json["fields"][1]["name"], "name");
        assert_eq!(json["fields"][0]["data_type"], "Int32");
        assert_eq!(json["fields"][1]["data_type"], "Utf8");
    }

    #[test]
    fn test_arrow_schema_to_json_invalid_file() {
        // Create a temp file with invalid contents
        let file = NamedTempFile::new().unwrap();
        std::fs::write(file.path(), b"not an arrow file").unwrap();

        let result = arrow_schema_to_json(file.path());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Failed to read Arrow IPC file"));
    }

    #[test]
    fn test_arrow_schema_to_json_nonexistent_file() {
        let result = arrow_schema_to_json("this_file_does_not_exist.arrow");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Failed to open file"));
    }
}
