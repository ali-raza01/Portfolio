
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CITY
from MY_DB.MY_SCHEMA.weather_data_model
where CITY is null



  
  
      
    ) dbt_internal_test