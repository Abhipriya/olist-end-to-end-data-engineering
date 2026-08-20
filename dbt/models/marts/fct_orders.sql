select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,

    datediff(
        'day',
        order_purchase_timestamp,
        order_delivered_customer_date
    ) as delivery_days,

    datediff(
        'day',
        order_estimated_delivery_date,
        order_delivered_customer_date
    ) as delivery_delay_days

from {{ ref('stg_orders') }}