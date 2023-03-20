WITH
time_taken_table AS(
SELECT
    -- timestamp
    TIMESTAMP_ADD(time_last_done, INTERVAL 8 HOUR) AS last_done_timestamp_TW,
    TIMESTAMP_ADD(time_started, INTERVAL 8 HOUR) AS started_timestamp_TW,
    TIMESTAMP_ADD(backup_timestamp, INTERVAL 8 HOUR) AS backup_timestamp_TW,
    TIMESTAMP_DIFF(time_last_done, time_started, SECOND) AS time,
    -- user user_id,
    user_id,
    -- behavior OF stack
    finished AS is_finished,
    cards_left AS left_quiz_cnt,
    cards_list,
    -- identification
    __key__.name AS stack_log_id,
    topic_id,
    article_id
FROM
    {{ source('datastore_backup', 'StackLog') }})
SELECT
last_done_timestamp_TW,
started_timestamp_TW,
backup_timestamp_TW,
user_id,
is_finished,
left_quiz_cnt,
cards_list,
stack_log_id,
topic_id,
article_id,
IF (time > 1800, 1800, time) AS time_taken
FROM
time_taken_table