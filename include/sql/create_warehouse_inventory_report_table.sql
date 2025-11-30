-- Creates warehouse_inventory_report table

create or replace table {{ params.database }}.{{ params.schema }}.warehouse_inventory_report
    (
        report_date date,
        warehouse_name string,
        product_name string,
        stock_level int,
        inbound_shipments int,
        outbound_shipments int,
        inventory_manager string,
        inventory_notes string
    );