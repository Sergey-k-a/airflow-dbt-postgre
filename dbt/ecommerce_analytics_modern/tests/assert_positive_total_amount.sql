-- Тест на положительные значения total_amount
select 
    order_id,
    total_amount
from {{ ref('stg_orders') }}
where total_amount < 0