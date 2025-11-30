
with daily_delivery_report as
(
    select 
        md5_number_lower64(product_name || product_category) as product_id,
        * 
    from {{ source('external_files', 'daily_delivery_report') }}
)

select
    product_id,
    product_name,
    product_category,
    product_subcategory,
    report_date as cdc_datetime
from 
    daily_delivery_report
qualify 
    row_number() over(partition by product_id order by report_date desc, order_number desc) = 1