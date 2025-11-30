{{
  config(
    materialized = 'incremental',
    unique_key = 'route_code',
    incremental_strategy = 'merge',
    on_schema_change = 'append_new_columns'
    )
}}

    with cur_route as
    (
        select 
            *
        from 
            {{ ref('stg_route') }}
    )

    select *
    from
        cur_route

    {% if is_incremental() %}

        where
            cdc_datetime > ( select coalesce(max(cdc_datetime), date_from_parts(1900, 1, 1)) from {{ this }} )

    {% endif %}
      