{ % macro customer_tier(total_orders) % }
    ASE 
        WHEN total_orders >= 100 THEN 'VIP'
        WHEN total_orders >= 50 THEN 'Gold'
        WHEN total_orders >= 20 THEN 'Silver'
        WHEN total_orders >= 5 THEN 'Bronze'
        ELSE 'New'
    END AS customer_tier
{ % endmacro %}