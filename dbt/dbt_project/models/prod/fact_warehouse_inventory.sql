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
    ),

    cur_product as
    (
        select *
        from   
            {{ ref('dim_product') }}
        qualify
            row_number() over(partition by product_name order by cdc_datetime desc) = 1
    )

    select 
        inventory.warehouse_inventory_id,
        product.product_id,
        inventory.stock_level,
        inventory.inbound_shipments,
        inventory.outbound_shipments,
        inventory.warehouse_name,
        inventory.cdc_datetime
    from
        cur_warehouse_inventory as inventory
    left join
        cur_product as product
        on product.product_name = product.product_name
    {% if is_incremental() %}

        where
            inventory.cdc_datetime > ( select coalesce(max(cdc_datetime), date_from_parts(1900, 1, 1)) from {{ this }} )

    {% endif %}