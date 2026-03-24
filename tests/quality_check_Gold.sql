/* 
Purpose:
--------
This script performs data quality checks on the Silver layer before loading data into the Gold layer.

Checks included:
1. Verify uniqueness of customer IDs (Primary Key validation)
2. Validate gender consolidation logic (CRM is the master source)
3. Evaluate creation of surrogate keys for dimension tables
4. Identify missing dimension references in the Sales fact data
*/

------------------------------------------------------------
-- 1. Check uniqueness of Customer ID (Primary Key validation)
--    Any returned rows indicate duplicate customer records
------------------------------------------------------------
SELECT 
    t.cst_id,
    COUNT(*) AS record_count
FROM (
    SELECT
        ca.cst_id,
        ca.cst_key,
        ca.cst_firstname,
        ca.cst_lastname,
        ca.cst_marital_status,
        ca.cst_gender,
        ca.cst_create_date,
        ea.bdate,
        ea.gen,
        eb.cntry
    FROM Silver.crm_cus_info ca
    LEFT JOIN Silver.erp_cust_az12 ea
        ON ca.cst_key = ea.cid
    LEFT JOIN Silver.erp_loc_a101 eb
        ON ca.cst_key = eb.cid
) t
GROUP BY t.cst_id
HAVING COUNT(*) > 1;

-- Expectation: No rows returned (PK uniqueness satisfied)


------------------------------------------------------------
-- 2. Validate gender consolidation logic
--    CRM gender is the master source unless it is 'N/A'
------------------------------------------------------------
SELECT DISTINCT
    ca.cst_gender AS crm_gender,
    ea.gen        AS erp_gender,

    CASE
        WHEN ca.cst_gender <> 'N/A' THEN ca.cst_gender
        ELSE COALESCE(ea.gen, 'N/A')
    END AS final_gender

FROM Silver.crm_cus_info ca
LEFT JOIN Silver.erp_cust_az12 ea
    ON ca.cst_key = ea.cid
LEFT JOIN Silver.erp_loc_a101 eb
    ON ca.cst_key = eb.cid;


------------------------------------------------------------
-- 3. Evaluate surrogate key generation
--    ROW_NUMBER ensures a unique sequential key for dimension tables
------------------------------------------------------------
SELECT
    ROW_NUMBER() OVER (
        ORDER BY ca.cst_create_date, ca.cst_id
    ) AS customer_key,

    ca.cst_id AS customer_id

FROM Silver.crm_cus_info ca
LEFT JOIN Silver.erp_cust_az12 ea
    ON ca.cst_key = ea.cid
LEFT JOIN Silver.erp_loc_a101 eb
    ON ca.cst_key = eb.cid;


------------------------------------------------------------
-- 4. Check referential integrity for Fact Sales
--    Identify sales records with missing customer or product dimensions
------------------------------------------------------------
SELECT
    cc.sls_ord_num   AS order_number,
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
    ON pro.product_number = cc.sls_prd_key

-- Rows returned indicate missing dimension records (data integrity issue)
WHERE cus.customer_id IS NULL
   OR pro.product_number IS NULL;
