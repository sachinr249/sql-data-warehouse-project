
/*
===============================================================================
DDL Script: Create silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'silver' Tables
===============================================================================
*/
IF OBJECT_ID('silver.crm_cust_info' , 'U') IS NOT NULL
     DROP TABLE silver.crm_cust_info;
go
create table silver.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE,
	dwh_crate_date DATETIME2 DEFAULT GETDATE()
);
go
  
IF OBJECT_ID('silver.crm_prd_info' , 'U') IS NOT NULL
     DROP TABLE  silver.crm_prd_info;
go
create table silver.crm_prd_info (
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost FLOAT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_crate_date DATETIME2 DEFAULT GETDATE()
);
go

IF OBJECT_ID('silver.crm_sales_details' , 'U') IS NOT NULL
     DROP TABLE  silver.crm_sales_details;
go
create table silver.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_crate_date DATETIME2 DEFAULT GETDATE()
);
go
  
IF OBJECT_ID('silver.erp_CUST_AZ12' , 'U') IS NOT NULL
     DROP TABLE  silver.erp_CUST_AZ12;
go
create table silver.erp_CUST_AZ12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(20),
	dwh_crate_date DATETIME2 DEFAULT GETDATE()
);
go
IF OBJECT_ID('silver.erp_LOC_A101' , 'U') IS NOT NULL
     DROP TABLE silver.erp_LOC_A101;
go
create table silver.erp_LOC_A101(
	cid NVARCHAR(50),
	cntry NVARCHAR(50),
	dwh_crate_date DATETIME2 DEFAULT GETDATE()
);
go

IF OBJECT_ID('silver.erp_PX_CAT_G1V2' , 'U') IS NOT NULL
     DROP TABLE silver.erp_PX_CAT_G1V2;
go
create table silver.erp_PX_CAT_G1V2(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50),
	dwh_crate_date DATETIME2 DEFAULT GETDATE()
);
go
