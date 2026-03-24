/* 
Purpose:
--------
This script creates the Gold layer views for the data warehouse.

1. dim_customer_info  -> Customer dimension with enriched demographic data
2. dim_product_info   -> Product dimension with category details
3. fact_sales         -> Sales fact table linking customers and products

These views transform and integrate cleaned data from the Silver layer
into analytical structures suitable for reporting and BI tools.
*/

------------------------------------------------------------
-- Create Customer Dimension View
------------------------------------------------------------
IF OBJECT_ID('Gold.dim_customer_info', 'V') IS NOT NULL
    DROP VIEW Gold.dim_customer_info;
GO

CREATE VIEW Gold.dim_customer_info AS
SELECT
    ROW_NUMBER() OVER (ORDER BY ca.cst_id) AS customer_key,  -- Surrogate key

    ca.cst_id            AS customer_id,
    ca.cst_key           AS customer_number,
    ca.cst_firstname     AS first_name,
    ca.cst_lastname      AS last_name,

    eb.cntry             AS country,
    ca.cst_marital_status AS marital_status,

    -- Use CRM gender unless it is 'N/A', then fallback to ERP gender
    CASE
        WHEN ca.cst_gender <> 'N/A' THEN ca.cst_gender
        ELSE COALESCE(ea.gen, 'N/A')
    END AS gender,

    ea.bdate             AS birth_date,
    ca.cst_create_date   AS created_date

FROM Silver.crm_cus_info ca
LEFT JOIN Silver.erp_cust_az12 ea
    ON ca.cst_key = ea.cid
LEFT JOIN Silver.erp_loc_a101 eb
    ON ca.cst_key = eb.cid;
GO


------------------------------------------------------------
-- Create Product Dimension View
------------------------------------------------------------
IF OBJECT_ID('Gold.dim_product_info', 'V') IS NOT NULL
    DROP VIEW Gold.dim_product_info;
GO

CREATE VIEW Gold.dim_product_info AS
SELECT
    -- Surrogate key ordered by start date and category
    ROW_NUMBER() OVER (
        ORDER BY cb.prd_start_dt, cb.prd_cat_id
    ) AS product_key,

    cb.prd_id        AS product_id,
    cb.prd_key       AS product_number,
    cb.prd_nm        AS product_name,

    cb.prd_cat_id    AS category_id,
    ec.cat           AS category_name,
    ec.subcat        AS subcategory_name,
    ec.maintainace   AS maintenance,

    cb.prd_cost      AS product_cost,
    cb.prd_line      AS product_line,
    cb.prd_start_dt  AS product_start_date

FROM Silver.crm_prd_info cb
LEFT JOIN Silver.erp_px_cat_g1v2 ec
    ON ec.id = cb.prd_cat_id

-- Only keep currently active products
WHERE cb.prd_end_dt IS NULL;
GO


------------------------------------------------------------
-- Create Sales Fact View
------------------------------------------------------------
IF OBJECT_ID('Gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW Gold.fact_sales;
GO

CREATE VIEW Gold.fact_sales AS
SELECT
    cc.sls_ord_num   AS order_number,

    -- Foreign keys to dimensions
    pro.product_key,
    cus.customer_key,

    cc.sls_order_dt  AS order_date,
    cc.sls_ship_dt   AS ship_date,
    cc.sls_due_dt    AS due_date,

    cc.sls_sales     AS sales,
    cc.sls_quantity  AS quantity,
    cc.sls_price     AS price

FROM Silver.crm_sales_details cc

LEFT JOIN Gold.dim_customer_info cus
    ON cus.customer_id = cc.sls_cust_id

LEFT JOIN Gold.dim_product_info pro
    ON pro.product_number = cc.sls_prd_key;
GO
