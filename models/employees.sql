select
    DATEDIFF ( year , birth_date, current_date ) as age,
    DATEDIFF ( year , hire_date, current_date ) as time_of_service,
    first_name || ' ' || last_name AS fullname,
    *
from {{ source("redshift", "employees") }}
