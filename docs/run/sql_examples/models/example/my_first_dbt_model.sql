
  
    

create or replace transient table MY_DB.MY_SCHEMA.my_first_dbt_model
    

    
    as (/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/



select
    CITY,
    TEMP_C,
    HUMIDITY,
    WIND_SPEED,
    DESCRIPTION
from MY_DB.MY_SCHEMA.weather_data_model
where TEMP_C is not null

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
    )
;


  