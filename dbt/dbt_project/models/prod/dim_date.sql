{{
    config(
        materialized = "table"
    )
}}

    with cte_raw_dim_date as
    (
        {{ cte_get_date_dimension("1990-01-01", "2050-12-31") }}
    )

    select 
        to_char(date_day, 'YYYYMMDD'):: int as date_id,
        *
    from
        cte_raw_dim_date