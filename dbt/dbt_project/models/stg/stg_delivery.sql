with daily_delivery_report as
(
    select * from {{ source('external_files', 'daily_delivery_report') }}
)

select 
    delivery.delivery_id,
    md5_number_lower64(delivery.customer_name || customer_address) as customer_id,
    delivery.route_code,
    to_char(delivery.shipment_date, 'YYYYMMDD'):: int as shipment_date_id,
    to_char(delivery.expected_delivery_date, 'YYYYMMDD'):: int as expected_delivery_date_id,
    to_char(delivery.actual_delivery_date, 'YYYYMMDD'):: int as actual_delivery_date_id,
    delivery.quantity,
    delivery.weight_kg,
    delivery.volume_cubic_meters,
    delivery.delivery_time_hours,
    delivery.distance_km,
    delivery.fuel_used_liters,
    delivery.delivery_cost_usd,
    delivery.insurance_cost_usd,
    delivery.late_delivery_penalty_usd,
    delivery.origin_warehouse_name,
    delivery.status,
    delivery.order_number,
    delivery.report_date as cdc_datetime
from
    daily_delivery_report as delivery
qualify 
    row_number() over(partition by delivery.delivery_id order by report_date desc) = 1

