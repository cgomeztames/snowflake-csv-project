with daily_delivery_report as
(
    select * from {{ source('external_files', 'daily_delivery_report') }}
),

cte_route as
(
    select
        route_code,
        origin_warehouse_name,
        destination_city,
        distance_km,
        traffic_condition,
        weather_condition,
        replace(driver_notes, '"', '') as driver_notes,
        ai_sentiment(replace(driver_notes, '"', ''), ['address', 'customer_at_home', 'traffic', 'weather']) as sentiment_note,
        report_date as cdc_datetime
    from
        daily_delivery_report
    qualify 
        row_number() over(partition by route_code order by report_date desc, order_number desc) = 1
)

    select 
        route_code,
        origin_warehouse_name,
        destination_city,
        distance_km
        traffic_condition,
        weather_condition,
        driver_notes,
        sentiment_note:categories[1].sentiment::varchar as address_sentiment,
        sentiment_note:categories[2].sentiment::varchar as customer_at_home_sentiment,
        sentiment_note:categories[3].sentiment::varchar as traffic_sentiment,
        sentiment_note:categories[4].sentiment::varchar as weather_sentiment,
        cdc_datetime
    from
        cte_route