{% macro estimated_discount(total_amount) %}
    CASE
        when {{ total_amount }} > 500 then {{ total_amount }} * 0.2
        else 0
    END
{% endmacro %}