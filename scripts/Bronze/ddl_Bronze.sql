/*
Purpose:
This script creates the Bronze layer tables used to store raw data from
CRM and ERP source systems.

Process:
- If a table already exists, it is dropped.
- A new table is then created with the required structure.
- These tables act as the initial landing zone for raw data before
  transformation in later layers (e.g., Silver and Gold).

Usage:
Typically executed during initial setup or when resetting the Bronze
schema structure in a Data Warehouse pipeline.
*/

-- Drop and recreate CRM customer information table
IF OBJECT_ID('Bronze.crm_cus_info', 'U') IS NOT NULL
    DROP TABLE Bronze.crm_cus_info;
GO

CREATE TABLE Bronze.crm_cus_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(12),
    cst_gender          NVARCHAR(10),
    cst_create_date     DATE
);
GO


-- Drop and recreate CRM product information table
IF OBJECT_ID('Bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE Bronze.crm_prd_info;
GO

CREATE TABLE Bronze.crm_prd_info (
    prd_id      INT,
    prd_key     NVARCHAR(50),
    prd_nm      NVARCHAR(50),
    prd_cost    DECIMAL,
    prd_line    NVARCHAR(8),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME       
);
GO


-- Drop and recreate CRM sales details table
IF OBJECT_ID('Bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE Bronze.crm_sales_details;
GO

CREATE TABLE Bronze.crm_sales_details (
    sls_ord_num   NVARCHAR(80),
    sls_prd_key   NVARCHAR(50),
    sls_cust_id   INT,
    sls_order_dt  INT,
    sls_ship_dt   INT,
    sls_due_dt    INT,
    sls_sales     INT,
    sls_quantity  INT,
    sls_price     DECIMAL(18,2)
);
GO


-- Drop and recreate ERP customer table
IF OBJECT_ID('Bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE Bronze.erp_cust_az12;
GO

CREATE TABLE Bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);
GO


-- Drop and recreate ERP location table
IF OBJECT_ID('Bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE Bronze.erp_loc_a101;
GO

CREATE TABLE Bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);
GO


-- Drop and recreate ERP product category table
IF OBJECT_ID('Bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE Bronze.erp_px_cat_g1v2;
GO

CREATE TABLE Bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintainace  NVARCHAR(50)
);
GO
