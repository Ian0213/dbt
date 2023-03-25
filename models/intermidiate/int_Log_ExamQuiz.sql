with
    exam_log_with_index as (
        select
            a.__key__.name as log_exam_id,
            a.exam_key_name as exam_id,
            if(a.is_custom_exam is null, false, a.is_custom_exam) as is_custom_exam,
            a.user_id as user_id,
            cards_list,
            row_number() over (partition by a.__key__.name) as exam_quiz_index
        from {{ ref("stg_ExamLog") }} as a
        left join
            (
                select * except (cards_list), concat(cards_lists, "}}") as cards_list
                from
                    {{ ref("stg_StackLog_from_DB") }},
                    unnest(
                        split(
                            replace(
                                replace(replace(cards_list, "{},", ""), "[", ""),
                                "]",
                                ""
                            ),
                            "}},"
                        )
                    ) as cards_lists
            ) as b
            on a.__key__.name = b.stack_log_id
        where a.user_id is not null and a.exam_key_name is not null
    ),
    exam_log_add_primary_key as (
        select
            log_exam_id,
            exam_id,
            is_custom_exam,
            a.user_id as user_id,
            user_primary_key,
            cards_list,
            exam_quiz_index
        from exam_log_with_index as a
        left join
            {{ ref("int_Relation_Users_Identification") }} as b on a.user_id = b.user_id
    ),
    exam_log_quiz_info as (
        select
            log_exam_id,
            exam_id,
            is_custom_exam,
            user_primary_key,
            user_id,
            replace(
                json_extract(cards_list, "$.card.exercise_name"), '"', ""
            ) as exercise,
            cast(
                trim(json_extract(cards_list, "$.card.quiz_pid"), '"') as int64
            ) as quiz_id,
            json_extract(cards_list, "$.card.skip") = 'true' as is_skip,
            json_extract(cards_list, "$.card.attempted") = 'true' as is_attempted,
            json_extract(cards_list, "$.card.correct_attempt") = 'true' as is_correct,
            exam_quiz_index
        from exam_log_add_primary_key
    )
select
    concat(
        log_exam_id,
        "-",
        ifnull(user_primary_key, "NULL"),
        "-",
        user_id,
        "-",
        exercise,
        "-",
        quiz_id,
        "-",
        cast(exam_quiz_index as string)
    ) as log_exam_quiz_id,
    *
from exam_log_quiz_info
