select count(*) as lines, company_name, contact_name
from {{ref('customers')}}
group by company_name, contact_name
having lines > 1