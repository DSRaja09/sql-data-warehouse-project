USE master;
GO

-- Check if the database already exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    PRINT 'Database already exists.';
END
ELSE
BEGIN
    CREATE DATABASE DataWarehouse;
END;
GO

-- Switch context to the target database
USE DataWarehouse;
GO

-- Create schemas for data layers
CREATE SCHEMA Bronze;
GO

CREATE SCHEMA Silver;
GO

CREATE SCHEMA Gold;
GO
