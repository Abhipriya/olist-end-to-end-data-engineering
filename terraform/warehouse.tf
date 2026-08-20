resource "snowflake_warehouse" "olist" {
  name           = "OLIST_WH"
  warehouse_size = "XSMALL"

  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true

  comment = "Warehouse for Olist Ecommerce pipeline"
}