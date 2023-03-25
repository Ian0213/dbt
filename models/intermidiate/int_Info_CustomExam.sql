with
    get_exam_info as (
        select
            __key__.name as custom_exam_id,
            name as custom_exam_title,
            creator.path as creator_path,
            creator.name as creator_key_name,
            timestamp_add(created_on, interval 8 hour) as create_timestamp_tw,
            public as is_public,
            count(*) as content_cnt,
            replace(
                string_agg(replace(array_to_string(content_list, ','), '"', "'")),
                '"',
                ''
            ) as content_list
        from {{ ref("stg_CustomExam") }}
        group by
            custom_exam_id,
            custom_exam_title,
            creator_path,
            creator_key_name,
            create_timestamp_tw,
            is_public
    ),
    -- tags
    get_tags as (
        select custom_exam_id, string_agg(tags, ',') as tags,
        from
            (
                select __key__.name as custom_exam_id, tags as tags
                from {{ ref("stg_CustomExam") }}, unnest(tags) as tags
            )
        group by custom_exam_id
    ),
    -- 合併 exam和 tags
    -- 根據 tags做出分類
    exam_info_join_tags as (
        select
            a.custom_exam_id as custom_exam_id,
            custom_exam_title,
            creator_path,
            creator_key_name,
            create_timestamp_tw,
            regexp_contains(tags, 'normal-exam') as is_normal_exam,
            regexp_contains(tags, 'pre-exam') as is_pre_exam,
            regexp_contains(tags, 'post-exam') as is_post_exam,
            regexp_contains(tags, 'monthly-exam') as is_monthly_exam,
            regexp_contains(tags, 'other-exam') as is_other_exam,
            is_public,
            content_cnt,
            content_list,
        from get_exam_info as a
        left join get_tags as b on a.custom_exam_id = b.custom_exam_id
    )
select
    custom_exam_id,
    custom_exam_title,
    user_primary_key as creator_user_primary_key,
    create_timestamp_tw,
    is_public,
    is_normal_exam,
    is_pre_exam,
    is_post_exam,
    is_monthly_exam,
    is_other_exam,
    content_cnt,
    content_list
from exam_info_join_tags as a
left join
    {{ ref("int_Relation_Users_Identification") }} as b
    on a.creator_key_name = b.key_name
