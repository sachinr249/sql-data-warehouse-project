/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		-- Loading silver.crm_cust_info
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';

	   insert into [silver].[crm_cust_info]
	       (cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
	    select 
		cst_id,
		cst_key,
		trim(cst_firstname) cst_firstname,
		trim(cst_lastname) cst_lastname,
		case 
			when upper(trim(cst_marital_status)) = 'M' then 'Married'
			when upper(trim(cst_marital_status)) ='S' then 'Single'
			else 'n/a'
		end as cst_marital_status,      --data normalization or standardization by mapping
	
		case 
			when upper(trim(cst_gndr)) = 'M' then 'Male'
			when upper(trim(cst_gndr)) ='F' then 'Female'
			else 'n/a'
		end as cst_gndr,
		cst_create_date
	from(
		select 
		*,
		row_number() over(partition by cst_id order by cst_create_date desc) flag
		from [bronze].[crm_cust_info]
		where cst_id is not null
		) t
	where flag =1 ; --select most recent record per customer
       SET @end_time = GETDATE();
	   PRINT'LOADING DURATION '+ cast(DATEDIFF(SECOND,@end_time,@start_time) as nvarchar) + ' seconds';
	   print'---------------';


	
      -- Loading crm_prd_info
     SET @start_time = GETDATE();
	 PRINT '>> Truncating Table: silver.crm_prd_info';
	 TRUNCATE TABLE silver.crm_prd_info;
	 PRINT '>> Inserting Data Into: silver.crm_prd_info';

	 insert into silver.crm_prd_info
		(prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt)
	select
		prd_id,
		replace(left(trim(prd_key),5) ,'-','_') cat_id,     --extracting the category id
		substring(trim(prd_key),7,len(prd_key)) prd_key,    -- extracting the product key
		prd_nm,
		coalesce(prd_cost,0) prd_cost,
		case upper(trim(prd_line))
			when 'R' then 'Road'
			when 'S' then 'Other Sales'
			when 'M' then 'Mountain'
			when 'T' then 'Tourism'
			else 'n/a'
		end prd_line,       -- Map product line codes to descriptive values
		prd_start_dt,
		dateadd(day,-1,lead(prd_start_dt) over(partition by prd_nm order by prd_start_dt))   prd_end_dt 
				 --- Calculate end date as one day before the next start date
	from bronze.crm_prd_info;
	   SET @end_time = GETDATE();
	   PRINT'LOADING DURATION '+ cast(DATEDIFF(SECOND,@end_time,@start_time) as nvarchar) + ' seconds';
	   print'---------------';

	       
	 -- Loading crm_sales_details
     SET @start_time = GETDATE();
	 PRINT '>> Truncating Table: silver.crm_sales_details';
	 TRUNCATE TABLE silver.crm_sales_details;
	 PRINT '>> Inserting Data Into: silver.crm_sales_details';

	INSERT INTO  silver.crm_sales_details
	(   sls_ord_num ,
		sls_prd_key ,
		sls_cust_id ,
		sls_order_dt ,
		sls_ship_dt ,
		sls_due_dt ,
		sls_sales ,
		sls_quantity ,
		sls_price 
		 )
	SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE 
		WHEN sls_order_dt =0 or len(sls_order_dt) !=8 THEN null
		ELSE cast(cast(sls_order_dt as nvarchar) as date)
	END sls_order_dt,
	CASE 
	   WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	   ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	CASE 
		WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	CASE 
		WHEN sls_sales<0 or sls_sales=0 or sls_price*sls_quantity != sls_sales THEN abs(sls_price*sls_quantity)
		ELSE sls_sales    -- Recalculate sales if original value is missing or incorrect
	END sls_sales,     
	sls_quantity,
	CASE 
		WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price  -- Derive price if original value is invalid
	END AS sls_price
	FROM bronze.crm_sales_details;

	   SET @end_time = GETDATE();
	   PRINT'LOADING DURATION '+ cast(DATEDIFF(SECOND,@end_time,@start_time) as nvarchar) + ' seconds';
	   print'---------------';

	   
	  PRINT '------------------------------------------------';
	  PRINT 'Loading ERP Tables';
	  PRINT '------------------------------------------------';
	  -- Loading silver.erp_CUST_AZ12
     SET @start_time = GETDATE();
	 PRINT '>> Truncating Table:silver.erp_CUST_AZ12';
	 TRUNCATE TABLE silver.erp_CUST_AZ12;
	 PRINT '>> Inserting Data Into: silver.erp_CUST_AZ12';

	insert into silver.erp_CUST_AZ12
		(cid,
		bdate,
		gen)
	select 
	case 
		when CID like 'NAS%' then substring(CID,4,len(CID))
		else CID
	end CID,
	case 
		when BDATE >= getdate() then null
		else BDATE 
	end BDATE,
	case
		when upper(trim(GEN)) in ('F' ,'FEMALE') then 'Female'
		when upper(trim(GEN)) in ('M' ,'MALE') then 'Male'
		else 'n/a'
	end GEN
	from bronze.erp_CUST_AZ12 ;

	   SET @end_time = GETDATE();
	   PRINT'LOADING DURATION '+ cast(DATEDIFF(SECOND,@end_time,@start_time) as nvarchar) + ' seconds';
	   print'---------------';

	
	 -- Loading silver.erp_loc_a101
     SET @start_time = GETDATE();
	 PRINT '>> Truncating Table:silver.erp_loc_a101';
	 TRUNCATE TABLE silver.erp_loc_a101;
	 PRINT '>> Inserting Data Into: silver.erp_loc_a101';

	INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry
		)
	SELECT
		REPLACE(cid, '-', '') AS cid, 
		CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
		END AS cntry -- Normalize and Handle missing or blank country codes
	FROM bronze.erp_loc_a101;

	   SET @end_time = GETDATE();
	   PRINT'LOADING DURATION '+ cast(DATEDIFF(SECOND,@end_time,@start_time) as nvarchar) + ' seconds';
	   print'---------------';

	
	 -- Loading silver.erp_PX_CAT_G1V2
     SET @start_time = GETDATE();
	 PRINT '>> Truncating Table:silver.erp_PX_CAT_G1V2';
	 TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
	 PRINT '>> Inserting Data Into: silver.erp_PX_CAT_G1V2';

	INSERT INTO silver.erp_PX_CAT_G1V2(
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
	   PRINT'LOADING DURATION '+ cast(DATEDIFF(SECOND,@end_time,@start_time) as nvarchar) + ' seconds';
	   print'---------------';

	   SET @batch_end_time=GETDATE();
	   PRINT'TOTAL LOADING DURATION '+ cast(DATEDIFF(SECOND,@batch_end_time,@batch_start_time) as nvarchar) + ' seconds';
	   print'---------------';
	END TRY
	BEGIN CATCH
	PRINT('an error occured ');
	print('eeror mesasge : '+ error_message());
	print('error number : ' + cast(error_number() as nvarchar));
	print('error line : ' + cast(error_line() as nvarchar));
	print('error procedure ' + error_procedure());
    END CATCH
END
GO
EXEC silver.load_silver
