/*
=====================================================================================================
Stored Procedure: Load Bronze Layer (Source System to Bronze Layer)
=====================================================================================================

Script Purpose: 
	This stored procedure loads data from the source system in the tables of the 'bronze' schema 
	from locally saved CSV files. Firstly, it truncates the bronze tables and after that the data
	are loaded with the method of 'BULK INSERT'.

Parameters: 
		None.
		This stored procedure does not accept any parameters nor return any values. 

Usage Example:
	EXECUTE bronze.load_bronze;

=====================================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @layer_start_time DATETIME, @layer_end_time DATETIME;
	BEGIN TRY
		SET @layer_start_time = GETDATE();

		PRINT '================================================================================================';
		PRINT 'Loading Data in the Bronze Layer'
		PRINT '================================================================================================';

		PRINT '------------------------------------------------------------------------------------------------';
		PRINT 'Loading Data in CRM Tables ';
		PRINT '------------------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data into Table: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\giann\OneDrive\Desktop\Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loading Duration is: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '------------------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data into Table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\giann\OneDrive\Desktop\Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loading Duration is: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '------------------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data into Table: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\giann\OneDrive\Desktop\Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loading Duration is: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '------------------------------------------------------------------------------------------------';


		PRINT '------------------------------------------------------------------------------------------------';
		PRINT 'Loading Data in ERP Tables ';
		PRINT '------------------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT 'Inserting Data into Table: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\giann\OneDrive\Desktop\Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loading Duration is: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '------------------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT 'Inserting Data into Table: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\giann\OneDrive\Desktop\Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loading Duration is: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '------------------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT 'Inserting Data into Table: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\giann\OneDrive\Desktop\Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loading Duration is: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '------------------------------------------------------------------------------------------------';

		SET @layer_end_time = GETDATE()
		PRINT '------------------------------------------------------------------------------------------------';
		PRINT 'Loading Duration of Whole Bronze Layer is: ' + CAST(DATEDIFF(second, @layer_start_time, @layer_end_time) AS NVARCHAR) + ' seconds';
		PRINT '------------------------------------------------------------------------------------------------';

	END TRY
	BEGIN CATCH 
		PRINT '================================================================================================';
		PRINT 'ERROR Occured during loading CRM & ERP TABLES';
		PRINT '================================================================================================';
	END CATCH
END 
