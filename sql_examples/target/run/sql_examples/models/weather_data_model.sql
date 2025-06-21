
  create or replace   view MY_DB.MY_SCHEMA.weather_data_model
  
   as (
    -- models/weather_data_model.sql

SELECT
    CITY,
    TEMP_C,
    HUMIDITY,
    WIND_SPEED,
    DESCRIPTION
FROM MY_DB.MY_SCHEMA.WEATHER_DATA
  );

