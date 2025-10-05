/*
===========================================================================================================
DDL Script: Create Bronze Tables
===========================================================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.Here I used DataWareHouse Database and i checked the tables every time after
    inserting a table and data.
	  Run this script to re-define the DDL structure of 'bronze' Tables
==========================================================================================================
*/

CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
);

BULK INSERT bronze.crm_cust_info
from 'C:\SQL\datasets\source_crm\cust_info.csv'
WITH (
 FIRSTROW = 2,
 FIELDTERMINATOR = ',',
 TABLOCK
);

SELECT * FROM bronze.crm_cust_info;

CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);

BULK INSERT bronze.crm_prd_info
from 'C:\SQL\datasets\source_crm\prd_info.csv'
WITH (
 FIRSTROW = 2,
 FIELDTERMINATOR = ',',
 TABLOCK
);

SELECT * FROM bronze.crm_prd_info;

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

BULK INSERT bronze.crm_sales_details
from 'C:\SQL\datasets\source_crm\sales_details.csv'
WITH (
 FIRSTROW = 2,
 FIELDTERMINATOR = ',',
 TABLOCK
);

SELECT * FROM bronze.crm_sales_details;

CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);

BULK INSERT bronze.erp_cust_az12
from 'C:\SQL\datasets\source_erp\cust_az12.csv'
WITH (
 FIRSTROW = 2,
 FIELDTERMINATOR = ',',
 TABLOCK
);

SELECT * FROM bronze.erp_cust_az12;

CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);

BULK INSERT bronze.erp_loc_a101
from 'C:\SQL\datasets\source_erp\loc_a101.csv'
WITH (
 FIRSTROW = 2,
 FIELDTERMINATOR = ',',
 TABLOCK
);

SELECT * FROM bronze.erp_loc_a101;

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);

BULK INSERT bronze.erp_px_cat_g1v2
from 'C:\SQL\datasets\source_erp\px_cat_g1v2.csv'
WITH (
 FIRSTROW = 2,
 FIELDTERMINATOR = ',',
 TABLOCK
);

SELECT * FROM bronze.erp_px_cat_g1v2;
