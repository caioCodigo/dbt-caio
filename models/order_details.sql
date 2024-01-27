select
    od.order_id,
    od.product_id,
    od.unit_price,
    od.quantity,
    pr.product_name,
    pr.supplier_id,
    pr.category_id,
    od.unit_price * od.quantity as order_total,
    (pr.unit_price * od.quantity) - order_total as discount
from {{source('redshift','order_details')}} as od
left join {{source('redshift','products')}} as pr on od.product_id = pr.product_id