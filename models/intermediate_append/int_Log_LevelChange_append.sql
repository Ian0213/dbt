select
    key_email as user_primary_key,
    exercise_name,
    update_time_tw,
    is_downgrade,
    is_upgrade,
    cast(level as int64) as level
from {{ ref("stg_log_level_change_v2_from_streaminglog") }}
{% if is_incremental() %}
where update_time_tw >= (select max(update_time_tw) from {{ this }})
{% endif %}
