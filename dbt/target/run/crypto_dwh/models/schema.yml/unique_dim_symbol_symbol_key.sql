
    
    -- Create target schema if it does not exist
  USE "CryptoDWH";
  DECLARE @dbt_schema_lock int;
  EXEC @dbt_schema_lock = sp_getapplock
    @Resource = 'dbt_create_schema_gold',
    @LockMode = 'Exclusive',
    @LockOwner = 'Session',
    @LockTimeout = 30000;
  BEGIN TRY
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
    BEGIN
      EXEC('CREATE SCHEMA "gold"')
    END
  END TRY
  BEGIN CATCH
    IF @dbt_schema_lock >= 0
      EXEC sp_releaseapplock @Resource = 'dbt_create_schema_gold', @LockOwner = 'Session';
    THROW;
  END CATCH
  IF @dbt_schema_lock >= 0
    EXEC sp_releaseapplock @Resource = 'dbt_create_schema_gold', @LockOwner = 'Session';

  
  

  
  EXEC('create view 
    "gold"."testview_eb513d5eee197696b63f73633f4aa286_9682"
   as 
    
    
    

select
    symbol_key as unique_field,
    count(*) as n_records

from "CryptoDWH"."gold"."dim_symbol"
where symbol_key is not null
group by symbol_key
having count(*) > 1



  ;')
  select
    
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select * from 
    "gold"."testview_eb513d5eee197696b63f73633f4aa286_9682"
  
  ) dbt_internal_test;

  EXEC('drop view 
    "gold"."testview_eb513d5eee197696b63f73633f4aa286_9682"
  ;')