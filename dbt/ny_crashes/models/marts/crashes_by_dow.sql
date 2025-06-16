select
    day_of_week,
    case day_of_week
        when 0 then 'Sun'
        when 1 then 'Mon'
        when 2 then 'Tue'
        when 3 then 'Wed'
        when 4 then 'Thu'
        when 5 then 'Fri'
        else 'Sat'
    end as day_of_week_name,
    count(day_of_week) as cnt
from {{ ref("stg_crash") }}
group by 1
