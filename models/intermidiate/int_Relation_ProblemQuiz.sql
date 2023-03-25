with
    prob_quiz as (
        select exercise, quiz_id
        from {{ ref("stg_ProblemLog_from_DB") }}
        group by exercise, quiz_id
    ),
    prob_quiz_cnt as (
        select exercise, count(distinct ifnull(quiz_id, "0")) as total_quiz_cnt,
        from prob_quiz
        group by exercise
    )
select
    concat(a.exercise, "-", ifnull(a.quiz_id, "NULL")) as relation_id,
    a.exercise as exercise,
    quiz_id,
    total_quiz_cnt
from prob_quiz as a
left join prob_quiz_cnt as b on a.exercise = b.exercise
