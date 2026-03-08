{{ config(materialized='table') }}

select * from {{ source('source_erp', 'cust_az12') }}