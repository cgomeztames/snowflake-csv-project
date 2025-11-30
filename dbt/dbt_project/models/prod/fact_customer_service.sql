{{
  config(
    materialized = 'incremental',
    unique_key = 'ticket_id',
    incremental_strategy = 'merge',
    on_schema_change = 'append_new_columns'
    )
}}

    with cur_customer_service as
    (
        select 
            *
        from 
            {{ ref('stg_customer_service') }}
    ),

    cur_delivery as
    (
        select 
            *
        from
            {{ ref('fact_delivery') }}
    )

    select 
        cus_service.ticket_id,
        cur_delivery.customer_id,
        cus_service.delivery_id,
        cus_service.ticket_open_date_id,
        cus_service.ticket_closed_date_id,
        cus_service.ticket_closed_datetime,
        cus_service.resolution_time_hours,
        cus_service.agent_name,
        cus_service.agent_notes,
        cus_service.status,
        cus_service.cdc_datetime
    from
        cur_customer_service as cus_service
    left join
        cur_delivery
        on cur_delivery.delivery_id = cus_service.delivery_id
    {% if is_incremental() %}

        where
            cus_service.cdc_datetime > ( select coalesce(max(cdc_datetime), date_from_parts(1900, 1, 1)) from {{ this }} )

    {% endif %}
