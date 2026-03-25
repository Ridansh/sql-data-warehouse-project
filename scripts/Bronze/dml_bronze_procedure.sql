/*
================================================================================
Purpose:
This script performs full data reloads into the Bronze layer tables by truncating
existing data and bulk inserting fresh data from source CSV files.

Details:
- Each table is truncated before loading to ensure no duplicate or stale data.
- Data is ingested from CRM and ERP source files.
- BULK INSERT is used for efficient high-volume data loading.
- FIRSTROW = 2 skips header rows in CSV files.
- FIELDTERMINATOR = ',' specifies comma-separated values.
- TABLOCK improves performance during bulk load operations.

Tables Covered:
- Bronze.crm_cust_info
- Bronze.crm_prd_info
- Bronze.crm_sales_details
- Bronze.erp_cust_az12
- Bronze.erp_loc_a101
- Bronze.erp_px_cat_g1v2

Notes:
- Ensure file paths are accessible from the SQL Server instance.
- This script is intended for initial loads or full refresh scenarios.
- Not suitable for incremental loads.

================================================================================
*/
CREATE OR ALTER PROCEDURE Bronze.load_bronze AS

PRINT('===========================================================');
PRINT('LOADING BRONZE LAYER');
PRINT('===========================================================');
BEGIN

	PRINT('----------------------------------');
	PRINT('LOADING CRM TABLES');
	PRINT('----------------------------------');
	
	PRINT('>>TRUNCATING TABLE:Bronze.crm_cust_info');
	TRUNCATE TABLE Bronze.crm_cust_info;

	PRINT('>>INSERTING DATA INTO: Bronze.crm_cust_info');
	BULK INSERT Bronze.crm_cust_info
	FROM 'C:\Users\ridansh.kaul\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);

	PRINT('>>TRUNCATING TABLE: Bronze.crm_prd_info');
	TRUNCATE TABLE Bronze.crm_prd_info;

	PRINT('>>INSERTING DATA INTO: Bronze.crm_prd_info');
	BULK INSERT Bronze.crm_prd_info
	FROM 'C:\Users\ridansh.kaul\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);

	PRINT('>>TRUNCATING TABLE: Bronze.crm_sales_details');
	TRUNCATE TABLE Bronze.crm_sales_details;

	PRINT('>>INSERTING DATA INTO: Bronze.crm_sales_details');
	BULK INSERT Bronze.crm_sales_details
	FROM 'C:\Users\ridansh.kaul\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);

	PRINT('----------------------------------');
	PRINT('LOADING CRM TABLES');
	PRINT('----------------------------------');


	PRINT('>>TRUNCATING TABLE: Bronze.erp_cust_az12');
	TRUNCATE TABLE Bronze.erp_cust_az12;

	PRINT('>>INSERTING DATA INTO: Bronze.erp_cust_az12');
	BULK INSERT Bronze.erp_cust_az12
	FROM 'C:\Users\ridansh.kaul\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
	WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);

	PRINT('>>TRUNCATING TABLE: Bronze.erp_loc_a101');
	TRUNCATE TABLE Bronze.erp_loc_a101;

	PRINT('>>INSERTING DATA INTO: Bronze.erp_loc_a101');
	BULK INSERT Bronze.erp_loc_a101
	FROM 'C:\Users\ridansh.kaul\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
	WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);

	PRINT('>>TRUNCATING TABLE: Bronze.erp_px_cat_g1v2');
	TRUNCATE TABLE Bronze.erp_px_cat_g1v2;

	PRINT('>>INSERTING DATA INTO: Bronze.erp_px_cat_g1v2');
	BULK INSERT Bronze.erp_px_cat_g1v2
	FROM 'C:\Users\ridansh.kaul\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
	WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);

END


EXEC Bronze.load_bronze;
