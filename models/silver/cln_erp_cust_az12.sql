{{ config(schema='silver', materialized='table') }}

select
    case
        when cid like 'NAS%' then substring(cid, 4, len(cid))
        else cid
    end as cid,

    case
        when bdate > getdate() then null
        else bdate
    end as bdate,

    case
        when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
        when upper(trim(gen)) in ('M', 'MALE') then 'Male'
        else 'n/a'
    end as gen

from {{ ref('stg_erp_cust_az12') }}
