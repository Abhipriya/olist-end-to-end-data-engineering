resource "snowflake_schema" "raw" {
  database = snowflake_database.olist.name
  name     = "RAW"

  comment = "Raw Olist data loaded by Airflow"
}

resource "snowflake_schema" "staging" {
  database = snowflake_database.olist.name
  name     = "STAGING"

  comment = "Cleaned and standardized data"
}

resource "snowflake_schema" "analytics" {
  database = snowflake_database.olist.name
  name     = "ANALYTICS"

  comment = "Business-ready analytics models created by dbt"
}