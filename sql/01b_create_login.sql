-- Least-privilege login used by Spark and dbt. Runs right after
-- 01_create_gold.sql (the database must exist) and before the scripts that
-- grant rights to it -- the file is named 01b so a plain alphabetical run
-- order is already the correct order.
--
-- :setvar APP_PASSWORD is supplied by docker/mssql/init.sh from
-- MSSQL_PASSWORD. CHECK_POLICY = OFF keeps a simple local password usable.
USE master;
GO
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = 'crypto_app')
    EXEC('CREATE LOGIN crypto_app WITH PASSWORD = ''$(APP_PASSWORD)'', CHECK_POLICY = OFF');
GO
USE CryptoDWH;
GO
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'crypto_app')
    CREATE USER crypto_app FOR LOGIN crypto_app;
GO
ALTER ROLE db_datareader ADD MEMBER crypto_app;
ALTER ROLE db_datawriter ADD MEMBER crypto_app;
GO
GRANT CREATE TABLE, CREATE VIEW TO crypto_app;
GRANT ALTER ON SCHEMA::gold TO crypto_app;
GO
