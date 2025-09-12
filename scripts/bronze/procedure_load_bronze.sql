/*
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
create or alter procedure bronze.load_bronze as 
	BEGIN
	DECLARE @BEGIN_TIME DATETIME ,@END_TIME DATETIME ,@BEGIN_BATCH_TIME DATETIME ,@END_BATCH_TIME DATETIME;
	
	BEGIN TRY
	    PRINT'============================'
		PRINT'    Loading bronze layer    '
		PRINT'============================'

		PRINT'------------------------------'
		PRINT'      LOADING CRM TABLE       '
		PRINT'------------------------------'
		SET @BEGIN_BATCH_TIME=GETDATE();
		SET @BEGIN_TIME=GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\sachin mewada\OneDrive\Desktop\source_crm\cust_info.csv'
		WITH(
		   FIRSTROW=2,
		   FIELDTERMINATOR =',',
		   TABLOCK
		);
		SET @END_TIME=GETDATE();
		PRINT'LOAD DURATION' + CAST(DATEDIFF(SECOND , @BEGIN_TIME , @END_TIME) AS NVARCHAR) + ' SECONDS';

		SET @BEGIN_TIME=GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\sachin mewada\OneDrive\Desktop\source_crm\prd_info.csv'
		WITH(
		   FIRSTROW=2,
		   FIELDTERMINATOR =',',
		   TABLOCK
		);
		SET @END_TIME=GETDATE();
		PRINT'LOAD DURATION' + CAST(DATEDIFF(SECOND , @BEGIN_TIME, @END_TIME) AS NVARCHAR) + ' SECONDS';

		SET @BEGIN_TIME=GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\sachin mewada\OneDrive\Desktop\source_crm\sales_details.csv'
		WITH(
		   FIRSTROW=2,
		   FIELDTERMINATOR =',',
		   TABLOCK
		);
		SET @END_TIME=GETDATE();
		PRINT'LOAD DURATION' + CAST(DATEDIFF(SECOND , @BEGIN_TIME , @END_TIME) AS NVARCHAR) + ' SECONDS';

	    PRINT'------------------------------'
		PRINT'      LOADING CRM TABLE       '
		PRINT'------------------------------'

		SET @BEGIN_TIME=GETDATE();
		TRUNCATE TABLE bronze.erp_CUST_AZ12;
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\sachin mewada\OneDrive\Desktop\source_erp\CUST_AZ12.csv'
		WITH(
		   FIRSTROW=2,
		   FIELDTERMINATOR =',',
		   TABLOCK
		);
		SET @END_TIME=GETDATE();
		PRINT'LOAD DURATION' + CAST(DATEDIFF(SECOND , @BEGIN_TIME, @END_TIME) AS NVARCHAR) + ' SECONDS';

		SET @BEGIN_TIME=GETDATE();
		TRUNCATE TABLE bronze.erp_LOC_A101;
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Users\sachin mewada\OneDrive\Desktop\source_erp\LOC_A101.csv'
		WITH(
		   FIRSTROW=2,
		   FIELDTERMINATOR =',',
		   TABLOCK
		);
		SET @END_TIME=GETDATE();
		PRINT'LOAD DURATION' + CAST(DATEDIFF(SECOND , @BEGIN_TIME, @END_TIME) AS NVARCHAR) + ' SECONDS';

		SET @BEGIN_TIME=GETDATE();
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\sachin mewada\OneDrive\Desktop\source_erp\PX_CAT_G1V2.csv'
		WITH(
		   FIRSTROW=2,
		   FIELDTERMINATOR =',',
		   TABLOCK
		);
		SET @END_TIME=GETDATE();
		PRINT'LOAD DURATION' + CAST(DATEDIFF(SECOND , @BEGIN_TIME , @END_TIME) AS NVARCHAR) + ' SECONDS';

		SET @END_BATCH_TIME=GETDATE();
		PRINT'===========================';
		PRINT'  BRONZE LOADING COMPLETED  ';
		PRINT'===========================';
		PRINT' TOTAL LOAD DURATION ' + CAST(DATEDIFF(SECOND, @BEGIN_BATCH_TIME,@END_BATCH_TIME) AS NVARCHAR ) +' SECONDS';

	END TRY
	BEGIN CATCH 
		print('an error occured ');
		print('error message' + error_message());
		print('error number' + cast(error_number() as NVARCHAR));
		print('error line' + cast(error_line() as NVARCHAR));
		PRINT('error procedure' + error_procedure() );
	END CATCH
END
GO
exec bronze.load_bronze
