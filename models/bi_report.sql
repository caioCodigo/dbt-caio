{{
    config(
        materialized='table',
        post_hook=["
            GRANT USAGE ON SCHEMA {{target.schema}} TO GROUP bigroup;
            GRANT SELECT ON TABLE {{target.schema}}.bi_report TO GROUP bigroup;
        "]
    )
}}

select {{retorna_campos()}} from {{ref('fact_table')}} where category_name = '{{var('category')}}'