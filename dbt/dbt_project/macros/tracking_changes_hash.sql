
{% macro tracking_changes_hash(model, columns_to_exclude=[]) %}
  {%- set relation = model -%}
  {%- set columns = adapter.get_columns_in_relation(relation) -%}
  {# Lowercase exclusion list #}
  {%- set exclude_lower = [] -%}
  {%- for c in columns_to_exclude -%}
    {%- do exclude_lower.append(c | lower) -%}
  {%- endfor -%}
  {# Build a list of columns to include #}
  {%- set include_cols = [] -%}
  {%- for col in columns -%}
    {%- if col.name | lower not in exclude_lower -%}
      {%- do include_cols.append(col.name) -%}
    {%- endif -%}
  {%- endfor -%}
  {# Join columns with "||" safely #}
  hash({{ include_cols | join(', ') }})
{% endmacro %}
