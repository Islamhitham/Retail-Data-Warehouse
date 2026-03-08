{{ config(materialized='table') }}

select * from {{ source('source_crm', 'sales_details') }}