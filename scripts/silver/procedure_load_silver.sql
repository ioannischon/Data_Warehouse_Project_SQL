/*
=====================================================================================================
Stored Procedure: Load Silver Layer (Bronze Layer to Silver Layer)
=====================================================================================================

Script Purpose: 
	This stored procedure loads cleaned data from the bronze 'schema' to the tables of the 'silver'  
	one after dealing with variouw modifications in terms of cleaning and standardizing the data.
	Firstly, it truncates the silver tables and after that the data are loaded with the method of 
	'INSERT INTO'.

Parameters: 
		None.
		This stored procedure does not accept any parameters nor return any values. 

Usage Example:
	EXECUTE silver.load_bronze;

=====================================================================================================
*/

ALTER   PROCEDURE [silver].[load_silver] AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @layer_start_time DATETIME, @layer_end_time DATETIME;
		BEGIN TRY
		SET @layer_start_time = GETDATE();

			PRINT '================================================================================================';
			PRINT 'Loading Cleaned Data in the Silver Layer'
			PRINT '================================================================================================';

			PRINT '------------------------------------------------------------------------------------------------';
			PRINT 'Loading Data in CRM Tables ';
			PRINT '------------------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.crm_cust_info';
			TRUNCATE TABLE silver.crm_cust_info;

			PRINT '>> Inserting Cleaned Data into Table: silver.crm_cust_info';

			INSERT INTO silver.crm_cust_info 
			(
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_create_date
			)

			SELECT 
				cst_id,
				cst_key,
				TRIM(cst_firstname) AS cst_firtstname,  -- Remove spaces from first name
				TRIM(cst_lastname) AS cst_lastname,     -- Remove spaces from last name
				CASE 
					WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
					WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
					ELSE 'n/a'                          
				END AS cst_marital_status,              -- Normalize marital status in readable format & Handling Missing Data
				CASE 
					WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
					WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
					ELSE 'n/a'
				END AS cst_gndr,                        -- Normalize gender in readable format & Handling Missing Data
				cst_create_date
				FROM (
					SELECT                                  -- Remove Duplicates from Primary Key and keeping the most recent documentation
						*,
						ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
					FROM bronze.crm_cust_info
					WHERE cst_id IS NOT NULL
				      )r WHERE flag_last = 1;

		SET @end_time = GETDATE();
			PRINT '>> Loading Duration is: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '------------------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.crm_prd_info';
			TRUNCATE TABLE silver.crm_prd_info;

			PRINT '>> Inserting Cleaned Data into Table: silver.crm_prd_info';
			INSERT INTO silver.crm_prd_info
			(
				prd_id, 
				cat_id,
				prd_key,
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt,
				prd_end_dt
			)
 
			SELECT
				prd_id,
				REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,  -- Creating the cat_id column to be able to join CRM & ERP Systems
				SUBSTRING(prd_key, 7,LEN(prd_key)) AS prd_key,
				prd_nm,
				ISNULL(prd_cost, 0) AS prd_cost,                        -- Replacing Nulls with numerical 0
				CASE 
					 WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
					 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
					 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
					 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
					 ELSE 'n/a'                                         -- Creating more readable values
				END AS prd_line,
				CAST(prd_start_dt AS DATE) AS prd_start_dt,
				CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt -- Replacing invalid End Dates with the previous date of the next record with the same prd_key
			FROM bronze.crm_prd_info;

		SET @end_time = GETDATE();
			PRINT '>> Loading Duration is: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '------------------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.crm_sales_details';
			TRUNCATE TABLE silver.crm_sales_details;

			PRINT '>> Inserting Cleaned Data into Table: silver.crm_sales_details';
			INSERT INTO silver.crm_sales_details
			(
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				sls_order_dt,
				sls_ship_dt,
				sls_due_dt,
				sls_sales,
				sls_quantity,
				sls_price
			)

			SELECT 
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				CASE 
					WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
				END AS sls_order_dt,
				CASE 
					WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
				END AS sls_ship_dt,
				CASE 
					WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
				END AS sls_due_dt,                                              -- Changing the Data type into DATE & handling invalid Dates as NULLS 
				CASE 
					WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price) 
					ELSE sls_sales
				END AS sls_sales,                                               -- Handling missing or negative Sales Values
				sls_quantity, 
				CASE 
					WHEN sls_price <= 0 OR sls_price IS NULL
					THEN sls_sales / NULLIF(sls_quantity, 0)
					ELSE sls_price
				END AS sls_price                                                -- Handling missing or negative Price Values
			FROM bronze.crm_sales_details;
	
		SET @end_time = GETDATE();
			PRINT '>> Loading Duration is: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '------------------------------------------------------------------------------------------------';

			PRINT '------------------------------------------------------------------------------------------------';
			PRINT 'Loading Cleaned Data in ERP Tables ';
			PRINT '------------------------------------------------------------------------------------------------';
	
		SET @start_time = GETDATE();
			PRINT 'Truncating Table: silver.erp_cust_az12';
			TRUNCATE TABLE silver.erp_cust_az12;

			PRINT 'Inserting Cleaned Data into Table: silver.erp_cust_az12';
			INSERT INTO silver.erp_cust_az12
				(
					cid,
					bdate,
					gen
				)

			SELECT 
				CASE 
					WHEN cid LIKE '%NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
					ELSE cid
				END AS cid,                                                    -- Removing Invalid id Values
				CASE 
					WHEN bdate > GETDATE() THEN NULL
					ELSE bdate                                                 -- Removing Invalid Birthday Values
				END AS bdate,                             
				CASE 
					WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
					WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
					ELSE 'n/a'
				END AS gen                                                     -- Creating more readable values
			FROM bronze.erp_cust_az12;

		SET @end_time = GETDATE();
			PRINT '>> Loading Duration is: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '------------------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
			PRINT 'Truncating Table: silver.erp_loc_a101';
			TRUNCATE TABLE silver.erp_loc_a101;

			PRINT 'Inserting Cleaned Data into Table: silver.erp_loc_a101';
			INSERT INTO silver.erp_loc_a101 
			(
				cid,
				cntry
			)

			SELECT 
				REPLACE (cid, '-', '') AS cid,                                 -- Removing the '-' symbol from the id Values
				CASE 
					WHEN TRIM(cntry) = 'DE' THEN 'GERMANY'
					WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
					WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
					ELSE TRIM(cntry)
				END AS cntry                                                   -- Normalizing & Handling missing Values for the country
			FROM bronze.erp_loc_a101;                                            

		SET @end_time = GETDATE();
			PRINT '>> Loading Duration is: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '------------------------------------------------------------------------------------------------';
	
		SET @start_time = GETDATE();
			PRINT 'Truncating Table: silver.erp_px_cat_g1v2';
			TRUNCATE TABLE silver.erp_px_cat_g1v2;

			PRINT 'Inserting Data into Table: silver.erp_px_cat_g1v2';
			INSERT INTO silver.erp_px_cat_g1v2
			(
				id,
				cat,
				subcat,
				maintenance
			)

			SELECT 
				id,
				cat,
				subcat,
				maintenance
			FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
			PRINT '>> Loading Duration is: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '------------------------------------------------------------------------------------------------';

			SET @layer_end_time = GETDATE()
			PRINT '------------------------------------------------------------------------------------------------';
			PRINT 'Loading Duration of Whole Silver Layer is: ' + CAST(DATEDIFF(second, @layer_start_time, @layer_end_time) AS NVARCHAR) + ' seconds';
			PRINT '------------------------------------------------------------------------------------------------';

		END TRY
		BEGIN CATCH 
			PRINT '================================================================================================';
			PRINT 'ERROR Occured during loading CRM & ERP TABLES';
			PRINT '================================================================================================';
		END CATCH
END
