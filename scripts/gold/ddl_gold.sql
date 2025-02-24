/*
=============================================================
DDL Script: Create Gold Tables
=============================================================

Script purpose:
	This script creates views in the 'gold' schema, 
	after aggregating data from the 'silver' tables. If the  
	viwes already exist, they are initially dropped. 
	The Gold layer concludes the final dimension and fact
	tables of the project and offers Data ready for analysis.

=============================================================
*/

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL 
	DROP VIEW gold.dim_customers;
GO 

CREATE VIEW gold.dim_customers AS 
	SELECT 
		ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
		cci.cst_id AS customer_id,
		cci.cst_key AS customer_number,
		cci.cst_firstname AS first_name,
		cci.cst_lastname AS last_name,
		ela.cntry AS country,
		CASE 
			WHEN cci.cst_gndr != 'n/a' THEN cci.cst_gndr
			ELSE COALESCE(eca.gen, 'n/a')
		END AS gender,                                      -- Integrating Data from 2 Systems, CRM is the Master Table for gender info
		cci.cst_marital_status AS marital_status,
		eca.bdate AS birthdate,
		cci.cst_create_date AS create_date		
	FROM silver.crm_cust_info cci
	LEFT JOIN silver.erp_cust_az12 eca
		ON cci.cst_key = eca.cid
	LEFT JOIN silver.erp_loc_a101 ela
		ON cci.cst_key = ela.cid;
GO

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL 
	DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
	SELECT 
		ROW_NUMBER() OVER (ORDER BY cpi.prd_start_dt, cpi.prd_key) AS product_key,
		cpi.prd_id AS product_id,
		cpi.prd_key AS product_number,
		cpi.prd_nm AS product_name,
		cpi.cat_id AS category_id,
		epcg.cat AS category,
		epcg.subcat AS subcategory,
		epcg.maintenance,
		cpi.prd_line AS product_line,
		cpi.prd_cost AS product_cost,
		cpi.prd_start_dt AS start_date
	FROM silver.crm_prd_info cpi
	LEFT JOIN silver.erp_px_cat_g1v2 epcg
		ON cpi.cat_id = epcg.id
	WHERE cpi.prd_end_dt IS NULL;                           -- Focusing only on Current Data
GO

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL 
	DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
	SELECT 
		csd.sls_ord_num AS order_number,
		gdp.product_key,
		gdc.customer_key,
		csd.sls_order_dt AS order_date,
		csd.sls_ship_dt AS shipping_date,
		csd.sls_due_dt AS due_date,
		csd.sls_sales AS sales,
		csd.sls_quantity AS quantity,
		csd.sls_price AS price
	FROM silver.crm_sales_details csd
	LEFT JOIN gold.dim_customers gdc
		ON csd.sls_cust_id = gdc.customer_id
	LEFT JOIN gold.dim_products gdp
		ON csd.sls_prd_key = gdp.product_number;
GO
