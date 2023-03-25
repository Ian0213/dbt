with
    log_level_change_v2 as (
        select
            * except (update_time),
            timestamp_add(update_time, interval 8 hour) as update_time_tw
        from {{ source("streaming_log", "log_level_change_v2") }}
    )
select *
from log_level_change_v2
