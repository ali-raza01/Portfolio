
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select city
from MY_DB.MY_SCHEMA.my_first_dbt_model
where city is null



  
  
      
    ) dbt_internal_test