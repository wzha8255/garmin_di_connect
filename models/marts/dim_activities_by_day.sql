with act_sum as (
    select * from {{ref('int_activities_summary')}}
)

select
    date(begin_timestamp_syd) as activity_date,
    activity_duration_min

from 
    act_sum
order by activity_date