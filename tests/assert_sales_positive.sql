-- Assert: no sales order line should have a sales_amount < 0
-- Returns offending rows; test fails if any rows are returned.
select order_number,
    sales_amount,
    quantity,
    price
from {{ ref('fct_sales') }}
where sales_amount < 0
