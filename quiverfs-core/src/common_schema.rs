use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileFormat {
    Arrow,
    Parquet,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableField {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    // Optionally add metadata or children for nested fields
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    pub fields: Vec<TableField>,
    pub format: FileFormat,
    // Optionally add schema-level metadata
}

pub trait SchemaInspectable {
    fn inspect_schema<P: AsRef<Path>>(path: P) -> Result<TableSchema, String>;
}

// Arrow implementation
mod arrow_schema_impl {
    use super::*;
    use arrow::ipc::reader::FileReader;
    use std::fs::File;
    use std::io::BufReader;

    pub struct ArrowSchemaInspector;

    impl SchemaInspectable for ArrowSchemaInspector {
        fn inspect_schema<P: AsRef<Path>>(path: P) -> Result<TableSchema, String> {
            let file = File::open(&path).map_err(|e| format!("Failed to open file: {}", e))?;
            let reader = FileReader::try_new(BufReader::new(file), None)
                .map_err(|e| format!("Failed to read Arrow IPC file: {}", e))?;
            let schema = reader.schema();
            let fields = schema
                .fields()
                .iter()
                .map(|f| TableField {
                    name: f.name().to_string(),
                    data_type: format!("{:?}", f.data_type()),
                    nullable: f.is_nullable(),
                })
                .collect();
            Ok(TableSchema {
                fields,
                format: FileFormat::Arrow,
            })
        }
    }
}

// Parquet implementation
mod parquet_schema_impl {
    use super::*;
    use parquet::file::reader::FileReader;
    use parquet::file::reader::SerializedFileReader;
    use parquet::schema::types::Type;
    use std::fs::File;

    pub struct ParquetSchemaInspector;

    fn extract_fields(schema: &Type) -> Vec<TableField> {
        match schema {
            Type::GroupType { fields, .. } => {
                fields.iter().flat_map(|f| extract_fields(f)).collect()
            }
            Type::PrimitiveType {
                basic_info,
                physical_type,
                ..
            } => {
                let name = schema.name().to_string();
                let data_type = format!("{:?}", physical_type);
                let nullable = basic_info.repetition() == parquet::basic::Repetition::OPTIONAL;
                vec![TableField {
                    name,
                    data_type,
                    nullable,
                }]
            }
        }
    }

    impl SchemaInspectable for ParquetSchemaInspector {
        fn inspect_schema<P: AsRef<Path>>(path: P) -> Result<TableSchema, String> {
            let file = File::open(&path).map_err(|e| format!("Failed to open file: {}", e))?;
            let reader = SerializedFileReader::new(file)
                .map_err(|e| format!("Failed to read Parquet file: {}", e))?;
            let schema = reader.metadata().file_metadata().schema();
            let fields = extract_fields(schema);
            Ok(TableSchema {
                fields,
                format: FileFormat::Parquet,
            })
        }
    }
}

// Re-export for use
pub use arrow_schema_impl::ArrowSchemaInspector;
pub use parquet_schema_impl::ParquetSchemaInspector;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_arrow_schema_inspect() {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{Field, Schema};
        use arrow::ipc::writer::FileWriter;
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

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

        let schema = ArrowSchemaInspector::inspect_schema(file.path()).unwrap();
        assert_eq!(schema.format, FileFormat::Arrow);
        assert_eq!(schema.fields[0].name, "id");
        assert_eq!(schema.fields[1].name, "name");
        assert_eq!(schema.fields[0].data_type, "Int32");
        assert_eq!(schema.fields[1].data_type, "Utf8");
        assert_eq!(schema.fields[0].nullable, false);
        assert_eq!(schema.fields[1].nullable, true);
    }

    #[test]
    fn test_parquet_schema_inspect() {
        use parquet::file::properties::WriterProperties;
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::parser::parse_message_type;
        use std::sync::Arc;

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

        let schema = ParquetSchemaInspector::inspect_schema(file.path()).unwrap();
        assert_eq!(schema.format, FileFormat::Parquet);
        assert_eq!(schema.fields[0].name, "id");
        assert_eq!(schema.fields[1].name, "name");
        assert_eq!(schema.fields[0].data_type, "INT32");
        assert_eq!(schema.fields[1].data_type, "BYTE_ARRAY");
        assert_eq!(schema.fields[0].nullable, false);
        assert_eq!(schema.fields[1].nullable, true);
    }
}
