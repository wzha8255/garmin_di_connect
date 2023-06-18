select 
    activityId as activity_id,
    activityType as activity_type,
    beginTimestamp as begin_timestamp,
    duration as activity_duration,
    avgSpeed as activity_avg_speed

from 
    {{source('garmin_di_connect','activities_summary')}}