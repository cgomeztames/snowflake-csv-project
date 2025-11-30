with customer_service_report as
(
    select * from {{ source('external_files', 'customer_service_report') }}
)
select 
    customer.ticket_id,
    -- ADD CUSTOMER_ID IN THE NEXT STEP USING DELIVERY_ID
    customer.delivery_id,
    to_char(customer.report_date, 'YYYYMMDD'):: int as ticket_open_date_id,
    to_char(dateadd(hour, zeroifnull(customer.resolution_time_hours),customer.report_date), 'YYYYMMDD'):: int as ticket_closed_date_id,
    dateadd(hour, zeroifnull(customer.resolution_time_hours),customer.report_date) as ticket_closed_datetime,
    customer.resolution_time_hours,
    customer.agent_name,
    replace(customer.agent_notes, '"', '') as agent_notes,
    customer.status,
    customer.report_date as cdc_datetime
from
    customer_service_report as customer
qualify 
    row_number() over(partition by customer.ticket_id order by report_date desc) = 1