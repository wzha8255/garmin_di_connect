select 
    customer_id,
    credit_score,
    country,
    gender,
    age,
    tenure,
    balance
from {{ref('stg_customers')}}