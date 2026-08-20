resource "snowflake_file_format_parquet" "olist_parquet" {
  name     = "OLIST_PARQUET_FORMAT"
  database = snowflake_database.olist.name
  schema   = snowflake_schema.raw.name

  comment = "Parquet file format for Olist data"
}