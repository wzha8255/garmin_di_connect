select 
    customer_id,
    credit_score,
    -- country, country field has been removed
    gender,
    age,
    tenure,
    balance
from {{ref('stg_customers')}}