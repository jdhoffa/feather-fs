use parquet::basic::Repetition;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::Type;
use serde_json::json;
use std::fs::File;
use std::path::Path;

/// Recursively extracts schema fields, including nested fields.
fn extract_fields(schema: &Type) -> Vec<serde_json::Value> {
    match schema {
        Type::GroupType { fields, .. } => fields.iter().flat_map(|f| extract_fields(f)).collect(),
        Type::PrimitiveType {
            basic_info,
            physical_type,
            ..
        } => {
            let name = schema.name();
            let type_str = format!("{:?}", physical_type);
            let nullable =
                !basic_info.has_repetition() || basic_info.repetition() == Repetition::OPTIONAL;
            vec![json!({
                "name": name,
                "type": type_str,
                "nullable": nullable,
            })]
        }
    }
}

/// Opens a Parquet file and returns its schema as JSON (field names, types, nullable flags).
pub fn parquet_schema_to_json<P: AsRef<Path>>(
    path: P,
) -> parquet::errors::Result<serde_json::Value> {
    let file = File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    let schema = reader.metadata().file_metadata().schema();

    let fields = extract_fields(schema);

    Ok(json!({ "fields": fields }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    #[test]
    fn test_parquet_schema_to_json_valid_file() {
        // Create a temporary Parquet file with a known schema, no rows needed
        let file = NamedTempFile::new().unwrap();
        let message_type = "
            message schema {
                REQUIRED INT32 id;
                OPTIONAL BYTE_ARRAY name (UTF8);
            }
        ";
        let schema = Arc::new(parse_message_type(message_type).unwrap());
        let props = Arc::new(WriterProperties::builder().build());
        let writer =
            SerializedFileWriter::new(file.reopen().unwrap(), schema.clone(), props).unwrap();
        writer.close().unwrap();

        // Test the function
        let json = parquet_schema_to_json(file.path()).unwrap();
        assert_eq!(json["fields"][0]["name"], "id");
        assert_eq!(json["fields"][1]["name"], "name");
        assert_eq!(json["fields"][0]["type"], "INT32");
        assert_eq!(json["fields"][1]["type"], "BYTE_ARRAY");
        assert_eq!(json["fields"][0]["nullable"], false);
        assert_eq!(json["fields"][1]["nullable"], true);
    }

    #[test]
    fn test_parquet_schema_to_json_invalid_file() {
        // Create a temp file with invalid contents
        let file = NamedTempFile::new().unwrap();
        std::fs::write(file.path(), b"not a parquet file").unwrap();

        let result = parquet_schema_to_json(file.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_parquet_schema_to_json_nonexistent_file() {
        let result = parquet_schema_to_json("this_file_does_not_exist.parquet");
        assert!(result.is_err());
    }
}
