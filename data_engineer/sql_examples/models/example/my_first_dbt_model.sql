
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

select
    CITY,
    TEMP_C,
    HUMIDITY,
    WIND_SPEED,
    DESCRIPTION
from {{ ref('weather_data_model') }}
where TEMP_C is not null

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
