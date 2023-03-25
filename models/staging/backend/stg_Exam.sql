with exam as (select * from {{ source("datastore_backup", "Exam") }}) select * from exam
