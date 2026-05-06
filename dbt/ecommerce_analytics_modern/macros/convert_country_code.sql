{% macro convert_country_code(country_column) %}
    CASE 
        WHEN {{ country_column }} = 'Russia' THEN 'RU'
        WHEN {{ country_column }} = 'USA' THEN 'US'
        WHEN {{ country_column }} = 'United States' THEN 'US'
        WHEN {{ country_column }} = 'UK' THEN 'GB'
        WHEN {{ country_column }} = 'United Kingdom' THEN 'GB'
        WHEN {{ country_column }} = 'Germany' THEN 'DE'
        WHEN {{ country_column }} = 'France' THEN 'FR'
        WHEN {{ country_column }} = 'Japan' THEN 'JP'
        ELSE 'Other'
    END
{% endmacro %}