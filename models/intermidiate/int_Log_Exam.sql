with
    exam_log as (
        select
            __key__.name as log_exam_id,
            exam_key_name as exam_id,
            user_id,
            if(is_custom_exam is null, false, is_custom_exam) is_custom_exam,
            count(exam_results) as total_results_cnt,
            countif(exam_results) as correct_cnt,
            safe_divide(countif(exam_results), count(exam_results)) as correct_rate,
        from {{ ref("stg_ExamLog") }}
        left join unnest(exam_results) as exam_results
        where exam_key_name is not null and user_id is not null
        group by log_exam_id, exam_id, user_id, is_custom_exam
    ),
    exam_log_add_stacklog_info as (
        select
            log_exam_id,
            exam_id,
            is_custom_exam,
            a.user_id as user_id,
            total_results_cnt,
            correct_cnt,
            correct_rate,
            last_done_timestamp_tw as time_last_done_tw,
            started_timestamp_tw as time_started_tw,
            is_finished,
            left_quiz_cnt,
        from exam_log as a
        left join
            {{ ref("stg_StackLog_from_DB") }} as b on a.log_exam_id = b.stack_log_id
    )
select
    log_exam_id,
    exam_id,
    is_custom_exam,
    user_primary_key,
    ifnull(total_results_cnt, 0) as total_results_cnt,
    ifnull(correct_cnt, 0) as correct_cnt,
    ifnull(correct_rate, 0) as correct_rate,
    time_last_done_tw,
    time_started_tw,
    case
        when timestamp_diff(time_last_done_tw, time_started_tw, second) < 1
        then 1
        else timestamp_diff(time_last_done_tw, time_started_tw, second)
    end as time_taken_sec,
    is_finished,
    ifnull(left_quiz_cnt, 0) as left_quiz_cnt
from exam_log_add_stacklog_info as a
left join {{ ref("int_Relation_Users_Identification") }} as b on a.user_id = b.user_id
