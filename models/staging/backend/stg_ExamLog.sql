with exam_log as (select * from {{ source("datastore_backup", "ExamLog") }})
select *
from exam_log
