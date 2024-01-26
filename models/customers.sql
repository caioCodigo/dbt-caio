with table_duplicated as (
    select *, 
        first_value(customer_id)
        over (partition by company_name, contact_name order by company_name rows between unbounded preceding and unbounded following ) as result
    from {{source('redshift','customers')}}
), removed as (
    select distinct(result) from table_duplicated
), final as (
    select * from {{source('redshift','customers')}} where customer_id in (select result from removed)
)

select * from final