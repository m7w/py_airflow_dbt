select
    borough,
    count(borough) as cnt
from {{ ref("stg_crash") }}
where borough is not null
group by 1
