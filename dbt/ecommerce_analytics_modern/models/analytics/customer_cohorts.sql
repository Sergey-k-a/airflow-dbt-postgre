{{
    config(
        materialized='table',
        tags=['analytics', 'cohorts']
    )
}}

WITH customers AS (
    SELECT 
        customer_id,
        DATE_TRUNC('month', registration_date) AS cohort_month
    FROM {{ ref('stg_customers') }}
),

orders AS (
    SELECT 
        customer_id,
        DATE_TRUNC('month', order_date) AS order_month,
        total_amount - estimated_discount AS net_revenue
    FROM {{ ref('stg_orders') }}
    WHERE order_category = 'successful'
),

cohort_size AS (
    SELECT 
        cohort_month,
        COUNT(DISTINCT customer_id) AS cohort_size
    FROM customers
    GROUP BY cohort_month
)

SELECT
    c.cohort_month,
    o.order_month,
    EXTRACT(YEAR FROM AGE(o.order_month, c.cohort_month)) * 12 + EXTRACT(MONTH FROM AGE(o.order_month, c.cohort_month)) AS period_number,
    COUNT(DISTINCT c.customer_id) AS active_customers,
    SUM(o.net_revenue) AS total_revenue,
    SUM(o.net_revenue) / COUNT(DISTINCT c.customer_id) AS avg_revenue_per_active_customer,
    
    -- Процент от когорты
    COUNT(DISTINCT c.customer_id) * 100.0 / MAX(cs.cohort_size) AS retention_rate

FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN cohort_size cs ON c.cohort_month = cs.cohort_month
GROUP BY c.cohort_month, o.order_month
ORDER BY c.cohort_month, o.order_month