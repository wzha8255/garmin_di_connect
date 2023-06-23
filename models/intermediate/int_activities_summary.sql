with stg_act as (
    select * from  {{ref('stg_activities_summary')}}
),
transformed_act as (
select  
    activity_id,
    activity_type,
    datetime(timestamp_millis(begin_timestamp), 'Australia/Sydney') as begin_timestamp_syd,
    activity_duration/1000/60 as activity_duration_min,
    activity_avg_speed
from 
   stg_act
)
select * from transformed_act
