
    
    

select
    city as unique_field,
    count(*) as n_records

from MY_DB.MY_SCHEMA.my_first_dbt_model
where city is not null
group by city
having count(*) > 1


