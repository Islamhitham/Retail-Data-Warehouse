{{ config(
    schema='gold',
    materialized='view'
) }}

select
    row_number() over (order by ci.cst_id) as customer_key,

    ci.cst_id            as customer_id,
    ci.cst_key           as customer_number,
    ci.cst_firstname     as first_name,
    ci.cst_lastname      as last_name,
    la.cntry             as country,
    ci.cst_marital_status as marital_status,

    case 
        when ci.cst_gndr != 'n/a' then ci.cst_gndr
        else coalesce(ca.gen, 'n/a')
    end as gender,

    ca.bdate             as birthdate,
    ci.cst_create_date   as create_date

from {{ ref('cln_crm_cust_info') }} ci

left join {{ ref('cln_erp_cust_az12') }} ca
    on ci.cst_key = ca.cid

left join {{ ref('cln_erp_loc_a101') }} la
    on ci.cst_key = la.cid
