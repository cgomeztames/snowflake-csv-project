-- Creates file format that will used for the all of them as they are the same type

    create file format if not exists {{ params.database }}.{{ params.schema }}.file_format_daily_files
        type = csv
        skip_header = 1
        null_if = ('#N/A')
        empty_field_as_null = true
        field_optionally_enclosed_by = '"'
    ;