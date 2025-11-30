{{
  config(
    materialized = 'incremental',
    unique_key = 'vehicle_id',
    incremental_strategy = 'merge',
    on_schema_change = 'append_new_columns'
    )
}}

{%- set max_date = "date_from_parts(9999, 1, 1)" -%}
{%- set min_date = "date_from_parts(1900, 1, 1)" -%}
{%- set model_stg = ref('stg_vehicle') -%}
{%- set natural_key = "vehicle_natural_key" -%}

{%- if is_incremental() -%}
    with cte_current_data as 
    (
        select 
            *,
            {{ tracking_changes_hash(model=this, columns_to_exclude=['cdc_datetime', 'valid_from', 'valid_to']) }} as changes_hash
        from
            {{ this }}
    ),

    cte_new_data as 
    (
        select 
            *,
            {{ tracking_changes_hash(model=model_stg, columns_to_exclude=['cdc_datetime']) }} as changes_hash
        from
            {{ model_stg }}
    ),
    
    cte_rows_to_update as
    (
        select 
            original.* exclude(valid_to, changes_hash),
            new_data.cdc_datetime as valid_to
        from
            cte_current_data as original
        inner join
            cte_new_data as new_data
            on  original.{{ natural_key }} = new_data.{{ natural_key }}
            and original.changes_hash !=  new_data.changes_hash
            and original.cdc_datetime < new_data.cdc_datetime
        where 1=1
            and original.valid_to = {{ max_date }}
    ),
    cte_new_rows as
    (
        select
            * exclude(changes_hash),
            cdc_datetime as valid_from,
            {{ max_date }} as valid_to
        from
            cte_new_data
        where
            cdc_datetime > ( select coalesce(max(cdc_datetime), {{ min_date }}) from {{ this }} )
    )
    
    select * from cte_new_rows
    
    union all
    
    select * from cte_rows_to_update

{% else %}
        
    with new_file as
    (
        select 
            *
        from 
            {{ model_stg }}
    )
    
    select 
        *,
        cdc_datetime as valid_from,
        {{ max_date }} as valid_to
    from 
        new_file

{%- endif -%}
