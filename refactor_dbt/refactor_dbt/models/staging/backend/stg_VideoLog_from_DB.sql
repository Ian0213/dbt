SELECT
TIMESTAMP_ADD(time_watched, INTERVAL 8 HOUR) AS active_timestamp_TW,
user.email AS user_primary_key,
video.path AS video_path,
ip_address,
is_video_completed,
last_second_watched,
points_earned,
seconds_watched AS time_taken_second,
youtube_id,
FROM
{{ source('datastore_backup', 'VideoLog') }}
WHERE
video.path IS NOT NULL