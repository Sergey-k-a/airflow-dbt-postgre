{{
  config(
    materialized='view',
    schema='staging',
    tags=['staging', 'customers']
  )
}}

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    registration_date,
    country,
    {{ convert_country_code('country') }} AS country_code,
    COALESCE(updated_at, registration_date) AS updated_at,
    
    -- Сегментация клиентов
    CASE 
        WHEN total_orders >= 100 THEN 'VIP'
        WHEN total_orders >= 50 THEN 'Gold'
        WHEN total_orders >= 20 THEN 'Silver'
        WHEN total_orders >= 5 THEN 'Bronze'
        ELSE 'New'
    END AS customer_tier,
    
    -- Дней с регистрации
    CURRENT_DATE - registration_date AS days_since_registration,
    
    -- Флаги
    CASE WHEN country IN ('Russia', 'USA', 'UK', 'Germany') THEN TRUE ELSE FALSE END AS is_major_market,
    
    -- Активность
    CASE 
        WHEN CURRENT_DATE - COALESCE(updated_at, registration_date) <= 30 THEN 'Active'
        WHEN CURRENT_DATE - COALESCE(updated_at, registration_date) <= 90 THEN 'Warm'
        WHEN CURRENT_DATE - COALESCE(updated_at, registration_date) <= 180 THEN 'Cold'
        ELSE 'Churned'
    END AS activity_status,
    
    -- Когорта по месяцу регистрации
    DATE_TRUNC('month', registration_date) AS cohort_month,
    
    -- Дополнительные поля из CSV
    total_orders,
    updated_at AS last_updated,
    
    -- Полное имя
    first_name || ' ' || last_name AS full_name

FROM {{ source('raw', 'customers') }}