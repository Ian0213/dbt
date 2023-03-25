{{
    config(
        partition_by={
            "field": "timestamp_TW",
            "data_type": "timestamp",
            "granularity": "Month",
        }
    )
}}
with
    problem_with_hint_after_attempt_list as (
        select
            active_timestamp_tw as timestamp_tw,
            user_id,
            exercise,
            quiz_id,
            hint_cnt,
            hint_after_attempt_list,
            row_number() over (
                partition by active_timestamp_tw order by hint_time_taken_list desc
            ) as hint_after_attempt_list_row_num
        from
            {{ ref("stg_ProblemLog_from_DB") }},
            unnest(hint_time_taken_list) as hint_time_taken_list
        where is_hint_used is true
    ),
    problem_with_hint_time_taken_list as (
        select
            active_timestamp_tw as timestamp_tw,
            user_id,
            exercise,
            quiz_id,
            hint_cnt,
            hint_time_taken_list,
            row_number() over (
                partition by active_timestamp_tw order by hint_after_attempt_list
            ) as hint_time_taken_list_row_num
        from
            {{ ref("stg_ProblemLog_from_DB") }},
            unnest(hint_after_attempt_list) as hint_after_attempt_list
        where is_hint_used is true
    ),
    problem_with_hint as (
        select
            a.timestamp_tw as timestamp_tw,
            a.user_id as user_id,
            a.exercise as exercise,
            a.quiz_id as quiz_id,
            a.hint_cnt as hint_cnt,
            ifnull(
                hint_time_taken_list[safe_ordinal(hint_after_attempt_list_row_num)],
                hint_time_taken_list[ordinal(array_length(hint_time_taken_list))]
            ) as hint_time_taken,
            ifnull(
                hint_after_attempt_list[safe_ordinal(hint_after_attempt_list_row_num)],
                hint_after_attempt_list[ordinal(array_length(hint_after_attempt_list))]
            ) as hint_after_attempt,
            hint_after_attempt_list_row_num as hint_index,
        from problem_with_hint_after_attempt_list as a
        join problem_with_hint_time_taken_list as b on a.timestamp_tw = b.timestamp_tw
        -- TODO: 加了下面幾行之後，結果的 rows 數量會減少，但不應該，要找時間搞清楚
        -- AND A.user_id = B.user_id
        -- AND A.exercise = B.exercise
        -- AND A.quiz_pid = B.quiz_pid
        -- AND A.hint_cnt = B.hint_cnt
        where hint_time_taken_list_row_num = hint_after_attempt_list_row_num
    ),
    problem_hint_merged_add_primary_key as (
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
                cast(hint_index as string)
            ) as log_problem_hint_id,
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
            hint_cnt,
            if
            (hint_time_taken > 1800, 1800, hint_time_taken) as hint_time_taken,
            hint_after_attempt,
            hint_index,
            timestamp_tw,
        from problem_with_hint as a
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
from problem_hint_merged_add_primary_key
