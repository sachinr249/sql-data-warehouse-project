/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
if object_id('gold.dim_customers', 'V') is not null
    drop view gold.dim_customers;
go

create view gold.dim_customers as
select 
	row_number() over( order by cu.cst_key ) as customer_key,   -- Surrogate key
	cu.cst_id as customer_id,
	cu.cst_key as customer_number,
	cu.cst_firstname as customer_firstname,
	cu.cst_lastname as customer_lastname,
	COALESCE(nullif(cu.cst_gndr,'n/a'),az.gen,'n/a') as customer_gender,  -- CRM is the primary source for gender
	lo.cntry  as customer_country,
	cu.cst_marital_status as customer_marital_status,
	cu.cst_create_date as customer_create_date
from silver.crm_cust_info cu
left join silver.erp_CUST_AZ12 az
    on az.cid=cu.cst_key
left join silver.erp_LOC_A101 lo
    on lo.cid=cu.cst_key;
go

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
if object_id('gold.dim_products', 'V') is not null
    drop view gold.dim_products;
go

create view gold.dim_products as
select 
	row_number() over (order by p.prd_start_dt, p.prd_key) as product_key,   -- Surrogate key
	p.prd_id as product_id,
	p.prd_key as product_number,
	p.prd_nm as product_name,
	p.cat_id as category_id,
	c.cat as category,
	c.subcat as subcategory,
	c.maintenance as maintenance,
	p.prd_cost as product_cost,
	p.prd_line as product_line,
	p.prd_start_dt as start_date
	from silver.crm_prd_info p
left join silver.erp_PX_CAT_G1V2 c
    on p.cat_id=c.id
where p.prd_end_dt is null ;  -- Filter out all historical data
go

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================

if object_id('gold.fact_sales','V') is not null
drop view gold.fact_sales;
go

create view gold.fact_sales as
select
    sd.sls_ord_num  as order_number,
    pr.product_key  as product_key,
    cu.customer_key as customer_key,
    sd.sls_order_dt as order_date,
    sd.sls_ship_dt  as shipping_date,
    sd.sls_due_dt as due_date,
    sd.sls_sales as sales_amount,
    sd.sls_quantity as quantity,
    sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
    on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu
    on sd.sls_cust_id = cu.customer_id;
go
