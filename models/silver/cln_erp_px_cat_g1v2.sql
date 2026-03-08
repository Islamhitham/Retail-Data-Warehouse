{{ config(schema='silver', materialized='table') }}

select
    id,
    cat,
    subcat,
    maintenance
from {{ ref('stg_erp_px_cat_g1v2') }}
