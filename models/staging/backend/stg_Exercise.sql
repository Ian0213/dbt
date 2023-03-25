with exercise as (select * from {{ source("datastore_backup", "Exercise") }})
select *
from exercise
