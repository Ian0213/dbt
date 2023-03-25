with custom_exam as (select * from {{ source("datastore_backup", "CustomExam") }})
select *
from custom_exam
