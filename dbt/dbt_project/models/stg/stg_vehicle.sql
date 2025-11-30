with vehicle_usage_report as
(
    select 
        md5_number_lower64(vehicle.license_plate || vehicle.report_date) as vehicle_id,
        md5_number_lower64(vehicle.license_plate) as vehicle_natural_key,
        *
        exclude (vehicle_id)
    from 
        {{ source('external_files', 'vehicle_usage_report') }} as vehicle
),

daily_delivery_report as
(
    select * from {{ source('external_files', 'daily_delivery_report') }}
)

select
    vehicle.vehicle_id,
    vehicle.vehicle_natural_key,
    delivery.driver_name,
    delivery.driver_license,
    delivery.carrier_name,
    vehicle.license_plate,
    vehicle.vehicle_type,
    vehicle.maintenance_cost_usd,
    vehicle.fuel_type,
    vehicle.report_date as cdc_datetime
from 
    vehicle_usage_report as vehicle
inner join
    daily_delivery_report as delivery
    on vehicle.license_plate = delivery.vehicle_license_plate
qualify
    row_number() over(partition by vehicle.vehicle_id order by delivery.report_date desc, delivery.order_number desc) = 1