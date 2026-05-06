{% macro price_segment(price_column) %}
    CASE 
        WHEN {{ price_column }} IS NULL THEN 'unknown'
        WHEN {{ price_column }} > 1000 THEN 'premium'
        WHEN {{ price_column }} > 100 THEN 'medium'
        ELSE 'budget'
    END
{% endmacro %}