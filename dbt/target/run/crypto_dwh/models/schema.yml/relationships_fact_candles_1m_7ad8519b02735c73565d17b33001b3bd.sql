
    
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
    "gold"."testview_6d3dd5dae2e016a4ef62d19318f28e76_18597"
   as 
    
    
    

with child as (
    select symbol_key as from_field
    from "CryptoDWH"."gold"."fact_candles_1m"
    where symbol_key is not null
),

parent as (
    select symbol_key as to_field
    from "CryptoDWH"."gold"."dim_symbol"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  ;')
  select
    
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select * from 
    "gold"."testview_6d3dd5dae2e016a4ef62d19318f28e76_18597"
  
  ) dbt_internal_test;

  EXEC('drop view 
    "gold"."testview_6d3dd5dae2e016a4ef62d19318f28e76_18597"
  ;')