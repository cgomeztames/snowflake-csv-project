{{
  config(
    materialized = 'incremental',
    unique_key = 'product_id',
    incremental_strategy = 'merge',
    on_schema_change = 'append_new_columns'
    )
}}

    with cur_product as
    (
        select 
            *
        from 
            {{ ref('stg_product') }}
    )

    select *
    from
        cur_product

    {% if is_incremental() %}

        where
            cdc_datetime > ( select coalesce(max(cdc_datetime), date_from_parts(1900, 1, 1)) from {{ this }} )

    {% endif %}
      