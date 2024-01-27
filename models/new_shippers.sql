select sh.*, se.shipper_email from {{source('redshift','shippers')}} as sh
left join {{ref('shippers_emails')}} as se on sh.shipper_id = se.shipper_id