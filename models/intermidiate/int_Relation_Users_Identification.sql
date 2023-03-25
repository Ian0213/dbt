with
    rank_duplicated_user_id as (
        select
            user_primary_key,
            user_id,
            rank_value,
            rank() over (partition by user_id order by rank_value desc) as rank
        from
            (
                select
                    user.email as user_primary_key,
                    user_id,
                    ifnull(
                        timestamp_diff(
                            last_login, timestamp('1970-01-01 00:00:00'), second
                        ),
                        0
                    ) + ifnull(
                        timestamp_diff(
                            last_activity, timestamp('1970-01-01 00:00:00'), second
                        ),
                        0
                    )
                    + points as rank_value,
                from {{ ref("stg_UserData") }}
                where
                    user_id in (
                        select user_id
                        from {{ ref("stg_UserData") }}
                        group by user_id
                        having count(*) > 1
                    )
            )
        order by user_id, rank
    ),
    no_used_user_primary_key as (
        select user_primary_key from rank_duplicated_user_id where rank != 1
    ),
    -- 找出所有的 UserData
    user_identify as (
        select
            user.email as user_primary_key,
            __key__.name as key_name,
            user_id as user_id,
            user_email as user_email,
            current_user.email as current_user_email,
            last_login
        from {{ ref("stg_UserData") }}
    ),
    -- 移除沒有在用的 user_primary_key
    rank_user_identify_by_login as (
        select
            user_primary_key,
            key_name,
            user_id,
            user_email,
            current_user_email,
            rank() over (partition by user_primary_key order by last_login desc) as rank
        from user_identify as a
        where
            not exists (
                select *
                from no_used_user_primary_key
                where user_primary_key = a.user_primary_key
            )
        group by
            user_primary_key,
            key_name,
            user_id,
            user_email,
            current_user_email,
            last_login
    )
select user_primary_key, key_name, user_id, user_email, current_user_email,
from rank_user_identify_by_login
where rank = 1
