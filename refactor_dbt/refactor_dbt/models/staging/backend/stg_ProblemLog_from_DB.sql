SELECT
-- timestamp
TIMESTAMP_ADD(time_done, INTERVAL 8 HOUR) AS active_timestamp_TW,
-- user
user.email AS user_primary_key,
user_id,
ip_address,
-- behavior of doing exercise
earned_proficiency AS proficiency_earned,
points_earned,
exercise,
quiz_pid AS quiz_id,
problem_number,
attempts,
count_attempts AS total_attempt_cnt,
correct AS is_correct,
skip AS is_skip,
hint_used AS is_hint_used,
count_hints AS hint_cnt,
hint_after_attempt_list,
hint_time_taken_list,
time_taken AS time_taken_second,
time_taken_attempts,
-- mode
IFNULL(pretest_mode, FALSE) AS is_on_pretest_mode,
IFNULL(exam_mode, FALSE) AS is_on_exam_mode,
IFNULL(review_mode, FALSE) AS is_on_review_mode
FROM
{{ source('datastore_backup', 'ProblemLog') }}