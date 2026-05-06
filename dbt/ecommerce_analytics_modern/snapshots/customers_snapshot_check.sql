
{% snapshot customers_snapshot_check %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['first_name', 'last_name', 'email', 'country', 'customer_tier'],
        invalidate_hard_deletes=True
    )
}}

SELECT
    *
FROM {{ ref('stg_customers') }}

{% endsnapshot %}