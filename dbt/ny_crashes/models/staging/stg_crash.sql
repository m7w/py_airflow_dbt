select
    crash_id,
    crash_date,
    borough,
    extract(dow from crash_date) as day_of_week,
    extract(hour from crash_date) as hour24
from crash
