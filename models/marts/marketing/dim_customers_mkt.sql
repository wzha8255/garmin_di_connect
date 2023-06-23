{{config(materialized="table")}}

select * from {{ref("dim_customers_versioning",version=1)}}