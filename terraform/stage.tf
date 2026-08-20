resource "snowflake_stage_internal" "olist_stage" {
  name     = "OLIST_STAGE"
  database = snowflake_database.olist.name
  schema   = snowflake_schema.raw.name

  comment = "Internal stage for Olist Parquet files"
}