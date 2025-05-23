/* 
=========================================================================
CREATE DATABASE AND SCHEMAS
=========================================================================
Script Purpose:
	This script creates a new database named 'DataWarehouse' after checking
	if it already exists. If the database exists, it is dropped and 
	recreated. Additionally, the script sets up three schemas within the 
	database: 'bronze', 'silver' and 'gold'.

WARNING: 
	Running this script will drop the entire 'DataWarehouse' database if exists.
	All data in the database will be deleted permanently. Proceed with
	caution and ensure you have proper backups before running this script.
*/


--DROP AND RECREATE THE DATABASE 'DataWarehouse'
IF EXIST(SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

--CREATE THE DATABASE 'DataWarehouse'
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;

--CREATE SCHEMAS
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
