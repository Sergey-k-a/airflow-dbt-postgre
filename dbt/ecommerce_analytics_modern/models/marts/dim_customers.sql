{{
    config(
        materialized='table',
        tags=['marts', 'customers', 'core']
    )
}}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

orders AS (
    SELECT 
        customer_id,
        order_id,
        order_date,
        total_amount,
        estimated_discount,
        total_amount - estimated_discount AS net_amount,
        quantity
    FROM {{ ref('stg_orders') }}
    WHERE order_category = 'successful'
),

-- RFM расчеты
rfm_calc AS (
    SELECT
        customer_id,
        MAX(order_date) AS last_order_date,
        COUNT(DISTINCT order_id) AS frequency,
        SUM(net_amount) AS monetary,
        AVG(net_amount) AS avg_order_value,
        MIN(order_date) AS first_order_date,
        (CURRENT_DATE - MAX(order_date))::integer AS recency,
        --COUNT(DISTINCT DATE_TRUNC('month', order_date)) AS active_months,
        COUNT(DISTINCT EXTRACT(YEAR FROM order_date) * 100 + EXTRACT(MONTH FROM order_date)) AS active_months,
        SUM(CASE WHEN EXTRACT(DOW FROM order_date) IN (0, 6) THEN 1 ELSE 0 END) AS weekend_orders,
        SUM(quantity) AS total_items_bought
    FROM orders
    GROUP BY customer_id
),

-- RFM сегментация
rfm_segmented AS (
    SELECT
        *,
        NTILE(4) OVER (ORDER BY recency DESC) AS r_score,  -- 4 = лучший
        NTILE(4) OVER (ORDER BY frequency) AS f_score,     -- 4 = лучший
        NTILE(4) OVER (ORDER BY monetary) AS m_score        -- 4 = лучший
    FROM rfm_calc
)

SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.registration_date,
    c.country,
    c.country_code,
    c.customer_tier,
    
    
    COALESCE(r.first_order_date, c.registration_date) AS first_order_date,
    r.last_order_date,
    COALESCE(r.frequency, 0) AS total_orders,
    COALESCE(r.monetary, 0) AS lifetime_value,
    COALESCE(r.avg_order_value, 0) AS avg_order_value,
    r.recency AS days_since_last_order,
    COALESCE(r.active_months, 0) AS active_months,
    COALESCE(r.total_items_bought, 0) AS total_items_bought,
    
    
    CONCAT(r.r_score, r.f_score, r.m_score) AS rfm_score,
    CASE 
        WHEN r.r_score >= 4 AND r.f_score >= 4 AND r.m_score >= 4 THEN 'Champions'
        WHEN r.r_score >= 3 AND r.f_score >= 3 AND r.m_score >= 3 THEN 'Loyal Customers'
        WHEN r.r_score >= 3 AND r.f_score >= 1 AND r.m_score >= 2 THEN 'Potential Loyalists'
        WHEN r.r_score >= 4 AND r.f_score <= 2 AND r.m_score <= 2 THEN 'New Customers'
        WHEN r.r_score <= 2 AND r.f_score >= 2 AND r.m_score >= 2 THEN 'At Risk'
        WHEN r.r_score <= 2 AND r.f_score <= 2 AND r.m_score <= 2 THEN 'Lost'
        ELSE 'Others'
    END AS rfm_segment,
    
    
    CASE 
        WHEN r.recency <= 30 THEN 'Active'
        WHEN r.recency <= 90 THEN 'Warm'
        WHEN r.recency <= 180 THEN 'Cold'
        ELSE 'Churned'
    END AS activity_status,
    
    
    CASE 
        WHEN r.recency > 90 AND c.customer_tier IN ('VIP', 'Gold') THEN 'High Risk'
        WHEN r.recency > 180 THEN 'Likely Churned'
        ELSE 'Active'
    END AS churn_risk

FROM customers c
LEFT JOIN rfm_segmented r ON c.customer_id = r.customer_id