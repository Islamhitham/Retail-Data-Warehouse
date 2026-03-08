{{ config(schema='silver', materialized='table') }}

with base as (

    select
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,

        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,

        try_cast(sls_sales as decimal(18,2))    as sls_sales,
        try_cast(sls_quantity as int)           as sls_quantity,
        try_cast(sls_price as decimal(18,2))    as sls_price

    from {{ ref('stg_crm_sales_details') }}

)

select
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    -- Order Date
    case 
        when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
        else try_cast(cast(sls_order_dt as varchar(8)) as date)
    end as sls_order_dt,

    -- Ship Date
    case 
        when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
        else try_cast(cast(sls_ship_dt as varchar(8)) as date)
    end as sls_ship_dt,

    -- Due Date
    case 
        when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
        else try_cast(cast(sls_due_dt as varchar(8)) as date)
    end as sls_due_dt,

    -- Sales Fix
    case 
        when sls_sales is null 
             or sls_sales <= 0 
             or sls_sales != sls_quantity * abs(sls_price)
        then sls_quantity * abs(sls_price)
        else sls_sales
    end as sls_sales,

    sls_quantity,

    -- Price Fix
    case 
        when sls_price is null or sls_price <= 0
        then sls_sales / nullif(sls_quantity, 0)
        else sls_price
    end as sls_price

from base
