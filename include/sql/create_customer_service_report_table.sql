-- Customer service table

    create table if not exists {{ params.database }}.{{ params.schema }}.customer_service_report (
        report_date date,
        ticket_id string,
        delivery_id string,
        customer_name string,
        issue_type string,
        resolution_time_hours number(10,2),
        status string,
        agent_name string,
        agent_notes string
    );