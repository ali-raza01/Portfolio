-- models/weather_data_model.sql

SELECT
    CITY,
    TEMP_C,
    HUMIDITY,
    WIND_SPEED,
    DESCRIPTION
FROM {{ source('MY_SCHEMA', 'WEATHER_DATA') }}