// to make sure we are able to use cortex ai in any region

    ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

// sets the role that can create and manage other roles
    use role useradmin;
    create role dbt_role;
    grant role dbt_role to role sysadmin; 

// creates monitors to control and track credit usage for the pipeline and analysts

    use role accountadmin;
    create resource monitor if not exists pipeline
    with 
        credit_quota = 2
        notify_users = (carlosgomez)
        frequency = daily
        start_timestamp = immediately
        triggers 
            on 75 percent do notify
            on 120 percent do notify;
            
    create resource monitor if not exists analyst
    with 
        credit_quota = 5
        notify_users = (carlosgomez)
        frequency = weekly
        start_timestamp = immediately
        triggers 
            on 75 percent do notify
            on 100 percent do suspend
            on 150 percent do suspend_immediate;

// switches to sysadmin to grant access and create warehouses for dbt and analysts
    use role sysadmin;
    create warehouse if not exists pipeline_wh
    with
        resource_monitor = pipeline
        warehouse_type = standard
        warehouse_size = xsmall
        resource_constraint = standard_gen_2
        max_cluster_count = 1
        auto_suspend = 300
        auto_resume = true
        initially_suspended = false
        comment = 'This warehouse was created purely to be used by dbt and the pipeline process'
        enable_query_acceleration = true
        query_acceleration_max_scale_factor = 2;

    create warehouse if not exists analyst_wh
    with
        resource_monitor = analyst
        warehouse_type = standard
        warehouse_size = xsmall
        resource_constraint = standard_gen_2
        min_cluster_count = 1
        max_cluster_count = 10
        scaling_policy = economy
        auto_suspend = 12000
        auto_resume = true
        initially_suspended = false
        comment = 'This warehouse was created for the analysts and bi tools';

    grant usage on warehouse pipeline_wh to role dbt_role;
    grant create database on account to role dbt_role;

    
// creates the databases that will store raw and transformed data
    use role dbt_role;
    create database if not exists load_db data_retention_time_in_days=7;
    create database if not exists warehouse_db data_retention_time_in_days=7;
    
// creates schemas under dbt_role to organize raw, staging, and transformed data

    create schema if not exists load_db.raw;
    create transient schema if not exists load_db.stg;
    create schema if not exists warehouse_db.dbo;

// since external stages are not an option, an internal stage is created
// also creates a stream to detect new files in the stage for incremental loads

    create stage if not exists load_db.raw.daily_files directory=(enable=true) encryption=(type='SNOWFLAKE_SSE');
    create stream if not exists load_db.raw.strm_daily_files on stage load_db.raw.daily_files;

// creates the pipeline user with its default role and warehouse

    use role useradmin;
    create user if not exists pipeline_user
        login_name = pipeline_user
        password = 'dbt_password123'
        default_role = dbt_role
        default_warehouse = pipeline_wh;
    
    use role securityadmin;
    grant role dbt_role to user pipeline_user;

// creates an analyst role and gives all the needed access

    use role useradmin;
    create role if not exists analyst_role;
    grant role analyst_role to role dbt_role;

    use role sysadmin;
    grant usage on warehouse analyst_wh to role analyst_role;

    use role dbt_role;
    grant usage on database warehouse_db to role analyst_role;
    
    grant usage on schema warehouse_db.dbo to role analyst_role;
    grant select on all tables in schema warehouse_db.dbo to role analyst_role;
    grant select on all views in schema warehouse_db.dbo to role analyst_role;
    grant usage on all procedures in schema warehouse_db.dbo to role analyst_role;

    grant all privileges on schema warehouse_db.public to role analyst_role;

// analyst user just for testing purposes

    use role useradmin;
    create user if not exists analyst_user
        login_name = analyst_user
        password = 'analyst_password123'
        default_role = analyst_role
        default_warehouse = analyst_wh;

    grant role analyst_role to user analyst_user;