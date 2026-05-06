{{
    config(
        materialized='table',
        tags=['marts', 'sales', 'core']
    )
}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
    WHERE order_category = 'successful'
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
)

SELECT
    -- Временные измерения
    DATE_TRUNC('day', o.order_date) AS sale_date,
    o.order_year,
    o.order_month,
    o.order_quarter,
    o.order_day_of_week,
    o.is_weekend,
    
    -- Измерения заказа
    o.order_id,
    o.order_category,
    o.quantity,
    o.total_amount,
    o.estimated_discount,
    o.total_amount - o.estimated_discount AS net_revenue,
    o.total_amount / NULLIF(o.quantity, 0) AS avg_item_price,
    
    -- Измерения клиента
    o.customer_id,
    c.country,
    c.country_code,
    c.customer_tier,
    c.is_major_market,
    
    -- Измерения продукта
    o.product_id,
    p.product_name,
    p.category,
    p.price_segment,
    p.price AS current_price,
    
    -- Маржинальность
    o.total_amount - o.estimated_discount - (p.price * o.quantity * 0.7) AS estimated_margin,
    
    -- Флаги для анализа
    CASE WHEN o.total_amount > 500 THEN TRUE ELSE FALSE END AS is_high_value,
    CASE WHEN o.quantity >= 3 THEN TRUE ELSE FALSE END AS is_bulk_order,
    CASE WHEN c.customer_tier IN ('VIP', 'Gold') THEN TRUE ELSE FALSE END AS is_vip_customer

FROM orders o
LEFT JOIN products p ON o.product_id = p.product_id
LEFT JOIN customers c ON o.customer_id = c.customer_id