with
    get_exam_info as (
        select
            __key__.name as exam_id,
            name as exam_title,
            public as is_public,
            (select count(*) from unnest(content_list)) as content_cnt,
            replace(
                string_agg(replace(array_to_string(content_list, ','), '"', "'")),
                '"',
                ''
            ) as content_list
        from {{ ref("stg_Exam") }}
        group by exam_id, exam_title, is_public, content_cnt
    ),
    -- tags
    get_tags as (
        select exam_id, string_agg(tags, ",") tags,
        from
            (
                select __key__.name as exam_id, tags as tags
                from {{ ref("stg_Exam") }}, unnest(tags) as tags
            )
        group by exam_id
    )
-- 合併 exam和 tags
-- 根據 tags做出分類
select
    a.exam_id exam_id,
    exam_title,
    tags,
    is_public,
    case
        when regexp_contains(tags, "topic-exam") then true else false
    end as is_topic_exam,
    case when regexp_contains(tags, "pre-exam") then true else false end as is_pre_exam,
    case
        when regexp_contains(tags, "post-exam") then true else false
    end as is_post_exam,
    content_cnt,
    content_list
from get_exam_info as a
left join get_tags as b on a.exam_id = b.exam_id
