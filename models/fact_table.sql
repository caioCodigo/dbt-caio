with dimension_products as (
    select
        pr.product_id,
        pr.product_name,
        pr.unit_price,
        sp.company_name as suppliers,
        c.category_name
    from {{source('redshift','products')}} as pr
    left join {{source('redshift','suppliers')}} as sp on pr.supplier_id = sp.supplier_id
    left join {{source('redshift','categories')}} as c on pr.category_id = c.category_id
), products_orders as (
    select 
        po.*,
        od.order_id,
        od.order_total,
        od.quantity,
        od.discount 
    from {{ref('order_details')}} as od
    inner join dimension_products as po on od.product_id = po.product_id
), dimension_orders as (
    select
    od.order_id,
    od.order_date,
    c.customer_id,
    c.company_name as customer,
    e.employee_id,
    e.fullname as employee,
    e.age,
    e.time_of_service,
    s.shipper_id
    from {{source('redshift','orders')}} as od
    left join {{ref('customers')}} as c on od.customer_id = c.customer_id
    left join {{ref('employees')}} as e on od.employee_id = e.employee_id
    left join {{source('redshift','shippers')}} as s on od.ship_via = s.shipper_id
), fact_table as (
    select 
        pr_or.*,
        di_or.order_date,
        di_or.customer,
        di_or.employee,
        di_or.age,
        di_or.time_of_service
    from dimension_orders as di_or
    left join products_orders as pr_or on di_or.order_id = pr_or.order_id
)

select * from fact_table