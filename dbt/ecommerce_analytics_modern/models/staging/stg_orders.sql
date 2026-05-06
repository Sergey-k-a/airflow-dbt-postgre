{{
    config(
        materialized='view',
        tags=['staging', 'orders']
    )
}}

SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    order_date,
    status,
    total_amount,
    
    -- Метрики заказа
    total_amount / NULLIF(quantity, 0) AS unit_price,
    
    -- Категоризация статусов
    CASE 
        WHEN status IN ('completed', 'shipped') THEN 'successful'
        WHEN status IN ('pending') THEN 'in_progress'
        WHEN status IN ('cancelled', 'refunded') THEN 'failed'
        ELSE 'unknown'
    END AS order_category,
    
    -- Временные метрики
    EXTRACT(YEAR FROM order_date) AS order_year,
    EXTRACT(MONTH FROM order_date) AS order_month,
    EXTRACT(QUARTER FROM order_date) AS order_quarter,
    EXTRACT(DOW FROM order_date) AS order_day_of_week,
    
    -- Скидка (улучшенная логика)
    CASE 
        WHEN total_amount > 1000 THEN total_amount * 0.15
        WHEN total_amount > 500 THEN total_amount * 0.10
        WHEN total_amount > 200 THEN total_amount * 0.05
        WHEN quantity >= 5 THEN total_amount * 0.03  -- оптовая скидка
        ELSE 0
    END AS estimated_discount,
    
    -- Флаг выходного дня
    CASE WHEN EXTRACT(DOW FROM order_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend

FROM {{ source('raw', 'orders') }}
WHERE order_date >= '2023-01-01'