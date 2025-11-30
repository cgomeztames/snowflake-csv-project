{{
  config(
    materialized = 'incremental',
    unique_key = 'customer_id',
    incremental_strategy = 'merge',
    on_schema_change = 'append_new_columns'
    )
}}

    with cur_customer as
    (
        select 
            *
        from 
            {{ ref('stg_customers') }}
    )

    select *
    from
        cur_customer

    {% if is_incremental() %}

        where
            cdc_datetime > ( select coalesce(max(cdc_datetime), date_from_parts(1900, 1, 1)) from {{ this }} )

    {% endif %}
      