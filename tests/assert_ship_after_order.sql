-- Assert: shipping date must not be earlier than order date
-- Returns offending rows; test fails if any rows are returned.
select order_number,
    order_date,
    shipping_date
from {{ ref('fct_sales') }}
where order_date is not null
    and shipping_date is not null
    and shipping_date < order_date
