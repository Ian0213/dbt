{{
    config(
        materialized="table",
        partition_by={
            "field": "timestamp_TW",
            "data_type": "timestamp",
            "granularity": "Month",
        },
    )
}}
with
    -- 先將 attempts 的順序抓出
    problem_with_attempts_position as (
        select
            active_timestamp_tw as timestamp_tw,
            user_id,
            exercise,
            quiz_id,
            if
            (
                time_taken_second > 1800, 1800, time_taken_second
            ) as total_attempt_time_taken,
            total_attempt_cnt,
            row_number() over (
                partition by active_timestamp_tw order by attempts
            ) as attempt_index,
            time_taken_attempts  -- 暫時先放著
        from {{ ref("stg_ProblemLog_from_DB") }}, unnest(attempts) as attempts
    ),
    problem_with_time_taken_attempts as (
        select
            active_timestamp_tw as timestamp_tw,
            user_id,
            exercise,
            quiz_id,
            attempts,
            row_number() over (
                partition by active_timestamp_tw order by time_taken_attempts
            ) as time_taken_attempts_index,
        from
            {{ ref("stg_ProblemLog_from_DB") }}, unnest(attempts) as time_taken_attempts
    ),
    problem_attempt as (
        select
            a.timestamp_tw as timestamp_tw,
            a.user_id as user_id,
            a.exercise as exercise,
            a.quiz_id as quiz_id,
            total_attempt_time_taken,
            total_attempt_cnt,
            attempt_index,
            if
            (
                ifnull(
                    time_taken_attempts[safe_ordinal(attempt_index)],
                    time_taken_attempts[ordinal(array_length(time_taken_attempts))]
                )
                > 1800,
                1800,
                ifnull(
                    time_taken_attempts[safe_ordinal(attempt_index)],
                    time_taken_attempts[ordinal(array_length(time_taken_attempts))]
                )
            ) as attempt_time_taken,
            ifnull(
                attempts[safe_ordinal(attempt_index)],
                attempts[ordinal(array_length(attempts))]
            ) as attempt,
        from problem_with_attempts_position as a
        join problem_with_time_taken_attempts as b on a.timestamp_tw = b.timestamp_tw
        where attempt_index = time_taken_attempts_index
    ),
    problem_attempt_merged_add_primary_key as (
        select
            concat(
                left(string(timestamp_tw), 26),
                "-",
                user_primary_key,
                "-",
                exercise,
                "-",
                ifnull(quiz_id, "NULL"),
                "-",
                cast(attempt_index as string)
            ) as log_problem_attempt_id,
            concat(
                left(string(timestamp_tw), 26),
                "-",
                user_primary_key,
                "-",
                exercise,
                "-",
                ifnull(quiz_id, "NULL")
            ) as log_problem_quiz_id,
            user_primary_key,
            exercise,
            quiz_id,
            total_attempt_time_taken,
            total_attempt_cnt,
            attempt_index,
            attempt_time_taken,
            attempt,
            timestamp_tw
        from problem_attempt as a
        left join
            {{ ref("int_Relation_Users_Identification") }} as b on a.user_id = b.user_id
    )
select
    *,
    extract(year from timestamp_tw) as year,
    extract(month from timestamp_tw) as month,
    date(timestamp_tw) as date,
    cast(format_date("%u", timestamp_tw) as int64) as weekday,
    extract(hour from timestamp_tw) as hour,
    div(extract(minute from timestamp_tw), 10) * 10 as ten_min_period
from problem_attempt_merged_add_primary_key
