/* 
=====================================================================
Create Database and Schemas
=====================================================================

Script Purpose:
	This script creates a new database named 'Datawarehouse' after checking if it already exists. If it does exist, it is dropped and created once again. Moreover, this script sets up three schemas in the database with the distinctive names: 'bronze', 'silver' & 'gold'.

WARNING:
	Running this particular script will drop the entire 'Datawarehouse' database if it exists. All data in the database will be DELETED permanently. Make sure you have backups before you proceed. 

*/




USE master; 
GO

-- Drop & Recreate the 'DataWarehouse' Database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN 
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create the Database DataWarehouse
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
