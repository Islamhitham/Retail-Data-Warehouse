{{ config(materialized='table') }}

select * from {{ source('source_crm', 'prd_info') }}