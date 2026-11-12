
    
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
    "gold"."testview_974bc4e614cd7392e80dbcecc98bcb88_17150"
   as 
    select *
from "CryptoDWH"."gold"."fact_candles_1m"
where high_price < low_price
   or open_price  not between low_price and high_price
   or close_price not between low_price and high_price
   or volume < 0
   or trade_count <= 0
  ;')
  select
    
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select * from 
    "gold"."testview_974bc4e614cd7392e80dbcecc98bcb88_17150"
  
  ) dbt_internal_test;

  EXEC('drop view 
    "gold"."testview_974bc4e614cd7392e80dbcecc98bcb88_17150"
  ;')