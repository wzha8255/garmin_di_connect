{{
  config(
    materialized = "table"
  )
}}

select 
  1 as customer_id, 
  'Mary' as customer_name, 
  cast('2019-01-01' as date) as first_transaction_date