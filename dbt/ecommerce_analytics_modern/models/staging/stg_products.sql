{{
    config(
        materialized='view',
        tags=['staging', 'products']
    )
}}

SELECT
    product_id,
    product_name,
    category,
    price,
    created_at,
    
    -- Классификация продуктов по цене через макрос
    {{ price_segment('price') }} AS price_segment,
    
    -- Дополнительные полезные поля
    CASE 
        WHEN price > 1000 THEN 'premium'
        WHEN price > 100 THEN 'medium'
        ELSE 'budget'
    END AS price_category,
    
    -- Возраст товара в днях
    (CURRENT_DATE - created_at)::integer as days_since_created,
    
    -- Флаг новинки (товары младше 30 дней)
    CASE 
        WHEN (CURRENT_DATE - created_at) <= 30 THEN TRUE 
        ELSE FALSE 
    END AS is_new_arrival,
    
    -- Категория для аналитики
    CASE 
        WHEN category IN ('Electronics', 'Home') THEN 'High Value'
        WHEN category IN ('Clothing') THEN 'Fashion'
        WHEN category IN ('Books') THEN 'Education'
        ELSE 'Other'
    END AS category_group

FROM {{ source('raw', 'products') }}