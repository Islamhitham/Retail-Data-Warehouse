{{ config(materialized='table') }}

select * from {{ source('source_erp', 'px_cat_g1v2') }}