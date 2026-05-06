{{
    config(
        materialized='incremental',
        unique_key='order_id', 
        incremental_strategy='delete+insert',                                
        tags=['incremental', 'orders']
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
)

select
    o.order_id,
    o.customer_id,
    o.product_id,
    c.first_name,
    c.last_name,
    p.product_name,
    p.category,
    o.quantity,
    o.order_date,
    o.status,
    o.total_amount,
    o.estimated_discount,
    o.total_amount - o.estimated_discount as net_amount,
    current_timestamp as loaded_at
    
from orders o
left join products p on o.product_id = p.product_id
left join customers c on o.customer_id = c.customer_id

{% if is_incremental() %}                                    -- Jinja-условие. Выполняется только при инкрементальном запуске (не при первом создании)
    -- Для merge нужно выбирать:
    -- 1. Новые заказы (по ID)
    -- 2. Изменённые заказы
    where not exists (
        select 1 
        from {{ this }} t 
        where t.order_id = o.order_id
    )
       or o.order_date >= (
           select  coalesce(max(order_date),  '1900-01-01')
           from {{ this }}
       )
{% endif %}

-- incremental_strategy='merge' для обновляемых данных доступна в >= 1.6 dbt

