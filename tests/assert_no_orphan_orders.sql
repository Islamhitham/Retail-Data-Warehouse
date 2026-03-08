-- Assert: every fact row must resolve to a known customer
-- Returns orphaned orders (customer_key IS NULL); test fails if any rows are returned.
select order_number,
    customer_key,
    product_key,
    order_date
from {{ ref('fct_sales') }}
where customer_key is null
