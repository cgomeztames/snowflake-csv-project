{% test negative_values(model, col) %}

    select * from {{ model }} where case when {{ col }} is null then 0 else {{ col }} end < 0

{% endtest %}