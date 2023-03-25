with
    relation_examcontent as (
        select
            __key__.name as exam_id,
            false as is_custom_exam,
            json_extract_scalar(content_list, '$.name') as content_id,
            cast(json_extract_scalar(content_list, '$.quiz_pid') as int64) as quiz_id,
        from {{ ref("stg_Exam") }}, unnest(content_list) as content_list
        union all
        select
            __key__.name as exam_id,
            true as is_custom_exam,
            json_extract_scalar(content_list, '$.name') as content_id,
            cast(json_extract_scalar(content_list, '$.quiz_pid') as int64) as quiz_id,
        from {{ ref("stg_CustomExam") }}, unnest(content_list) as content_list
    )
select *
from relation_examcontent
