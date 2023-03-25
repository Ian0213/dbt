with user_data as (select * from {{ source("datastore_backup", "UserData") }})
select *
from user_data
