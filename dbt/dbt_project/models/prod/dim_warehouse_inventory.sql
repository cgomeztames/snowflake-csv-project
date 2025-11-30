{{
  config(
    materialized = 'incremental',
    unique_key = 'warehouse_inventory_id',
    incremental_strategy = 'merge',
    on_schema_change = 'append_new_columns'
    )
}}

    with cur_warehouse_inventory as
    (
        select 
            *
        from 
            {{ ref('stg_warehouse_inventory') }}
    )

    select 
        warehouse_inventory_id,
        inventory_manager,
        inventory_notes,
        sentiment_damage_items,
        sentiment_discrepancy_items,
        sentiment_inventory_items,
        sentiment_stock_items,
        cdc_datetime
    from
        cur_warehouse_inventory
    {% if is_incremental() %}

        where
            cdc_datetime > ( select coalesce(max(cdc_datetime), date_from_parts(1900, 1, 1)) from {{ this }} )

    {% endif %}