{{
    config(
        materialized='table',
        tags=['analytics', 'reporting']
    )
}}

WITH sales AS (
    SELECT * FROM {{ ref('fct_daily_sales') }}
)

SELECT
    -- Временные агрегаты
    sale_date,
    order_year,
    order_month,
    order_quarter,
    
    -- География
    country,
    country_code,
    is_major_market,
    
    -- Продуктовые разрезы
    category,
    price_segment,
    
    -- Основные метрики
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(quantity) AS total_items_sold,
    SUM(total_amount) AS gross_revenue,
    SUM(net_revenue) AS net_revenue,
    SUM(estimated_margin) AS estimated_margin,
    
    -- Средние показатели
    AVG(total_amount) AS avg_order_value,
    AVG(net_revenue / NULLIF(quantity, 0)) AS avg_revenue_per_item,
    SUM(net_revenue) / NULLIF(COUNT(DISTINCT customer_id), 0) AS avg_revenue_per_customer,
    
    -- Показатели качества
    SUM(CASE WHEN is_high_value THEN 1 ELSE 0 END) AS high_value_orders,
    SUM(CASE WHEN is_bulk_order THEN 1 ELSE 0 END) AS bulk_orders,
    SUM(CASE WHEN is_vip_customer THEN 1 ELSE 0 END) AS vip_orders,
    
    -- Доля метрик
    SUM(CASE WHEN is_high_value THEN net_revenue ELSE 0 END) / NULLIF(SUM(net_revenue), 0) AS high_value_revenue_share,
    SUM(CASE WHEN is_weekend THEN net_revenue ELSE 0 END) / NULLIF(SUM(net_revenue), 0) AS weekend_revenue_share,
    
    -- Тренды (оконные функции)
    SUM(net_revenue) - LAG(SUM(net_revenue), 7) OVER (PARTITION BY country, category ORDER BY sale_date) AS weekly_revenue_change,
    SUM(net_revenue) / NULLIF(LAG(SUM(net_revenue), 7) OVER (PARTITION BY country, category ORDER BY sale_date), 0) - 1 AS weekly_growth_rate,
    
    -- Скользящие средние
    AVG(SUM(net_revenue)) OVER (PARTITION BY country, category ORDER BY sale_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7day_avg_revenue

FROM sales
GROUP BY 
    sale_date, order_year, order_month, order_quarter,
    country, country_code, is_major_market,
    category, price_segment
ORDER BY sale_date DESC, gross_revenue DESC