{{ config(materialized='table') }}

select * from {{ source('source_crm', 'cust_info') }}