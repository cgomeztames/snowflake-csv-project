with daily_delivery_report as
(
    select 
        md5_number_lower64(customer_name || customer_address) as customer_id,
        *
    from 
        {{ source('external_files', 'daily_delivery_report') }}
)

select
    customer_id,
    customer_name,
    customer_address,
    customer_city,
    customer_country
    postal_code,
    report_date as cdc_datetime
from 
    daily_delivery_report
qualify 
    row_number() over(partition by customer_id order by report_date desc, order_number desc) = 1