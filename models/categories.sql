{{ config(
    materialized='incremental',
    unique_key='category_id'
) }}

select * from {{source('redshift','categories')}}