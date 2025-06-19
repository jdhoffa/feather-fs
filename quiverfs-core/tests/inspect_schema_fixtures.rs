use quiverfs_core::common_schema::{
    ArrowSchemaInspector, ParquetSchemaInspector, SchemaInspectable,
};
use std::path::Path;

fn fixture_path(name: &str) -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

#[test]
fn test_arrow_schema_inspect_fixture() {
    let path = fixture_path("example.arrow");
    let schema = ArrowSchemaInspector::inspect_schema(&path).expect("Arrow schema parse failed");

    // Example: id: int32, name: utf8 (nullable), meta: struct<nested: int64>, ts: timestamp
    assert_eq!(schema.fields[0].name, "id");
    assert_eq!(schema.fields[0].data_type, "Int32");
    assert!(schema.fields[0].nullable);

    assert_eq!(schema.fields[1].name, "name");
    assert_eq!(schema.fields[1].data_type, "Utf8");
    assert!(schema.fields[1].nullable);

    assert_eq!(schema.fields[2].name, "meta");
    assert!(schema.fields[2].data_type.contains("nested"));

    assert_eq!(schema.fields[3].name, "ts");
    assert!(schema.fields[3].data_type.contains("Timestamp"));
}

#[test]
fn test_parquet_schema_inspect_fixture() {
    let path = fixture_path("example.parquet");
    let schema =
        ParquetSchemaInspector::inspect_schema(&path).expect("Parquet schema parse failed");

    // Example: id: INT32, name: BYTE_ARRAY (nullable), meta.nested: INT64, ts: INT64 (TIMESTAMP)
    assert_eq!(schema.fields[0].name, "id");
    assert_eq!(schema.fields[0].data_type, "INT32");
    assert!(schema.fields[0].nullable);

    assert_eq!(schema.fields[1].name, "name");
    assert_eq!(schema.fields[1].data_type, "BYTE_ARRAY");
    assert!(schema.fields[1].nullable);

    assert_eq!(schema.fields[2].name, "nested");
    assert_eq!(schema.fields[2].data_type, "INT64");

    assert_eq!(schema.fields[3].name, "ts");
    assert_eq!(schema.fields[3].data_type, "INT64"); // Parquet stores timestamp as INT64
}
