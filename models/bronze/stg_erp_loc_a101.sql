{{ config(materialized='table') }}

select * from {{ source('source_erp', 'loc_a101') }}