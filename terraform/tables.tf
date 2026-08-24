resource "snowflake_table" "raw_customers" {
  database = snowflake_database.olist.name
  schema   = snowflake_schema.raw.name
  name     = "RAW_CUSTOMERS"

  column {
    name = "customer_id"
    type = "VARCHAR"
  }

  column {
    name = "customer_unique_id"
    type = "VARCHAR"
  }

  column {
    name = "customer_zip_code_prefix"
    type = "NUMBER(38,0)"
  }

  column {
    name = "customer_city"
    type = "VARCHAR"
  }

  column {
    name = "customer_state"
    type = "VARCHAR"
  }
}


resource "snowflake_table" "raw_orders" {
  database = snowflake_database.olist.name
  schema   = snowflake_schema.raw.name
  name     = "RAW_ORDERS"

  column {
    name = "order_id"
    type = "VARCHAR"
  }

  column {
    name = "customer_id"
    type = "VARCHAR"
  }

  column {
    name = "order_status"
    type = "VARCHAR"
  }

  column {
    name = "order_purchase_timestamp"
    type = "VARCHAR"
  }

  column {
    name = "order_approved_at"
    type = "VARCHAR"
  }

  column {
    name = "order_delivered_carrier_date"
    type = "VARCHAR"
  }

  column {
    name = "order_delivered_customer_date"
    type = "VARCHAR"
  }

  column {
    name = "order_estimated_delivery_date"
    type = "VARCHAR"
  }
}


resource "snowflake_table" "raw_order_items" {
  database = snowflake_database.olist.name
  schema   = snowflake_schema.raw.name
  name     = "RAW_ORDER_ITEMS"

  column {
    name = "order_id"
    type = "VARCHAR"
  }

  column {
    name = "order_item_id"
    type = "NUMBER(38,0)"
  }

  column {
    name = "product_id"
    type = "VARCHAR"
  }

  column {
    name = "seller_id"
    type = "VARCHAR"
  }

  column {
    name = "shipping_limit_date"
    type = "VARCHAR"
  }

  column {
    name = "price"
    type = "FLOAT"
  }

  column {
    name = "freight_value"
    type = "FLOAT"
  }
}


resource "snowflake_table" "raw_order_payments" {
  database = snowflake_database.olist.name
  schema   = snowflake_schema.raw.name
  name     = "RAW_ORDER_PAYMENTS"

  column {
    name = "order_id"
    type = "VARCHAR"
  }

  column {
    name = "payment_sequential"
    type = "NUMBER(38,0)"
  }

  column {
    name = "payment_type"
    type = "VARCHAR"
  }

  column {
    name = "payment_installments"
    type = "NUMBER(38,0)"
  }

  column {
    name = "payment_value"
    type = "FLOAT"
  }
}


resource "snowflake_table" "raw_products" {
  database = snowflake_database.olist.name
  schema   = snowflake_schema.raw.name
  name     = "RAW_PRODUCTS"

  column {
    name = "product_id"
    type = "VARCHAR"
  }

  column {
    name = "product_category_name"
    type = "VARCHAR"
  }

  column {
    name = "product_name_lenght"
    type = "FLOAT"
  }

  column {
    name = "product_description_lenght"
    type = "FLOAT"
  }

  column {
    name = "product_photos_qty"
    type = "FLOAT"
  }

  column {
    name = "product_weight_g"
    type = "FLOAT"
  }

  column {
    name = "product_length_cm"
    type = "FLOAT"
  }

  column {
    name = "product_height_cm"
    type = "FLOAT"
  }

  column {
    name = "product_width_cm"
    type = "FLOAT"
  }
}