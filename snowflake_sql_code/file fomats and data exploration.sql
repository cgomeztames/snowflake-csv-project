use role dbt_role;

// Check the new files in the stream and all files in the stage

    select * from load_db.raw.strm_daily_files where metadata$action = 'INSERT'
    list @load_db.raw.daily_files;

// Create file format that will used for the all of them as they are the same type

    create or replace file format load_db.raw.file_format_daily_files
        type = csv
        skip_header = 1
        null_if = ('#N/A')
        empty_field_as_null = true
        field_optionally_enclosed_by = '"'

// Customer service exploration and table creation

    select 
        f.$1::date as report_date,
        f.$2 as ticket_id,
        f.$3 as delivery_id,
        f.$4 as customer_name,
        f.$5 as issue_type,
        f.$6::number(10,2) as resolution_time_hours,
        f.$7 as status,
        f.$8 as agent_name,
        f.$9 as agent_notes,
        metadata$filename as file_name,
        metadata$file_last_modified as last_modified
    from 
        @load_db.raw.daily_files
        (
            file_format => 'load_db.raw.file_format_daily_files',
            pattern => 'customer_service_report.*\\.csv'
        ) as f;


    create table if not exists load_db.raw.customer_service_report (
        report_date date,
        ticket_id string,
        delivery_id string,
        customer_name string,
        issue_type string,
        resolution_time_hours number(10,2),
        status string,
        agent_name string,
        agent_notes string
    );

    truncate table load_db.raw.customer_service_report;
    copy into load_db.raw.customer_service_report
        from 
            (
            select 
                f.$1::date as report_date,
                f.$2 as ticket_id,
                f.$3 as delivery_id,
                f.$4 as customer_name,
                f.$5 as issue_type,
                f.$6::number(10,2) as resolution_time_hours,
                f.$7 as status,
                f.$8 as agent_name,
                f.$9 as agent_notes
            from 
                @load_db.raw.daily_files
                (
                    file_format => 'load_db.raw.file_format_daily_files',
                    pattern => 'customer_service_report.*\\.csv'
                ) as f
        )


// Daily delivery report exploration and table creation

    select
        f.$1::date as report_date,
        f.$2 as delivery_id,
        f.$3::int as order_number,
        f.$4::date as shipment_date,
        f.$5::date as expected_delivery_date,
        f.$6::date as actual_delivery_date,
        f.$7 as customer_name,
        f.$8 as customer_address,
        f.$9 as customer_city,
        f.$10 as customer_country,
        f.$11 as postal_code,
        f.$12 as product_name,
        f.$13 as product_category,
        f.$14 as product_subcategory,
        f.$15::integer as quantity,
        f.$16::float as weight_kg,
        f.$17::float as volume_cubic_meters,
        f.$18 as vehicle_type,
        f.$19 as vehicle_license_plate,
        f.$20 as driver_name,
        f.$21 as driver_license,
        f.$22 as carrier_name,
        f.$23 as origin_warehouse_name,
        f.$24 as destination_city,
        f.$25 as route_code,
        f.$26 as status,
        f.$27::float as delivery_time_hours,
        f.$28::float as distance_km,
        f.$29::float as fuel_used_liters,
        f.$30::float as delivery_cost_usd,
        f.$31::float as insurance_cost_usd,
        f.$32::float as late_delivery_penalty_usd,
        f.$33 as traffic_condition,
        f.$34 as weather_condition,
        f.$35 as driver_notes,
        f.$36 as customer_feedback
    from
        @load_db.raw.daily_files
        (
            file_format => 'load_db.raw.file_format_daily_files',
            pattern => 'daily_delivery_report.*'
        ) as f;
    
    
    create table if not exists load_db.raw.daily_delivery_report 
    (
        report_date date,
        delivery_id varchar,
        order_number int,
        shipment_date date,
        expected_delivery_date date,
        actual_delivery_date date,
        customer_name varchar,
        customer_address varchar,
        customer_city varchar,
        customer_country varchar,
        postal_code varchar,
        product_name varchar,
        product_category varchar,
        product_subcategory varchar,
        quantity integer,
        weight_kg float,
        volume_cubic_meters float,
        vehicle_type varchar,
        vehicle_license_plate varchar,
        driver_name varchar,
        driver_license varchar,
        carrier_name varchar,
        origin_warehouse_name varchar,
        destination_city varchar,
        route_code varchar,
        status varchar,
        delivery_time_hours float,
        distance_km float,
        fuel_used_liters float,
        delivery_cost_usd float,
        insurance_cost_usd float,
        late_delivery_penalty_usd float,
        traffic_condition varchar,
        weather_condition varchar,
        driver_notes varchar,
        customer_feedback varchar
    );

// Vehicle usage report exploration and table creation

    select 
        f.$1::date as report_date,
        f.$2 as vehicle_id,
        f.$3 as license_plate,
        f.$4 as vehicle_type,
        f.$5::number(12,2) as mileage_km,
        f.$6 as fuel_type,
        f.$7::number(10,2) as fuel_consumed_liters,
        f.$8::number(10,2) as maintenance_cost_usd,
        f.$9::int as breakdowns_count,
        f.$10 as maintenance_notes
    from 
        @load_db.raw.daily_files
        (
            file_format => 'load_db.raw.file_format_daily_files',
            pattern => 'vehicle_usage_report.*\\.csv'
        ) as f;
        
    create table if not exists load_db.raw.vehicle_usage_report 
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

// Warehouse inventroy report data exploration and table creation

    select 
        f.$1::date as report_date,
        f.$2 as warehouse_name,
        f.$3 as product_name,
        f.$4::int as stock_level,
        f.$5::int as inbound_shipments,
        f.$6::int as outbound_shipments,
        f.$7 as inventory_manager,
        f.$8 as inventory_notes
    from 
        @load_db.raw.daily_files
        (
            file_format => 'load_db.raw.file_format_daily_files',
            pattern => 'warehouse_inventory_report.*\\.csv'
        ) as f;

    create or replace table load_db.raw.warehouse_inventory_report 
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


