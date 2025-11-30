with warehouse_inventory_report as
(
    select * from {{ source('external_files', 'warehouse_inventory_report') }}
),

cte_inventory as
(
    select 
        md5_number_lower64(inventory.warehouse_name || inventory.product_name) as warehouse_inventory_id,
        inventory.product_name,
        inventory.stock_level,
        inventory.inbound_shipments,
        inventory.outbound_shipments,
        inventory.warehouse_name,
        inventory.inventory_manager,
        inventory.inventory_notes,
        ai_sentiment(replace(inventory.inventory_notes, '"', ''), ['damage_items', 'discrepancy', 'inventory', 'stock']) as sentiment_note,
        inventory.report_date as cdc_datetime
    from
        warehouse_inventory_report as inventory
)

select 
    inventory.warehouse_inventory_id,
    inventory.product_name,
    inventory.stock_level,
    inventory.inbound_shipments,
    inventory.outbound_shipments,
    inventory.warehouse_name,
    -- CREATE A SEPARATE DIM BASED ON THE COLUMNS BELLOW
    inventory.inventory_manager,
    inventory.inventory_notes,
    inventory.sentiment_note:categories[1].sentiment::varchar as sentiment_damage_items,
    inventory.sentiment_note:categories[2].sentiment::varchar as sentiment_discrepancy_items,
    inventory.sentiment_note:categories[3].sentiment::varchar as sentiment_inventory_items,
    inventory.sentiment_note:categories[4].sentiment::varchar as sentiment_stock_items,
    inventory.cdc_datetime
from
    cte_inventory as inventory
qualify
    row_number() over(partition by inventory.warehouse_inventory_id order by inventory.cdc_datetime desc) = 1