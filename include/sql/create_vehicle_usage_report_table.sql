-- Creates vehicle_usage_report table

create table if not exists {{ params.database }}.{{ params.schema }}.vehicle_usage_report 
    (
        report_date date,
        vehicle_id string,
        license_plate string,
        vehicle_type string,
        mileage_km number(12,2),
        fuel_type string,
        fuel_consumed_liters number(10,2),
        maintenance_cost_usd number(10,2),
        breakdowns_count int,
        maintenance_notes string
    );