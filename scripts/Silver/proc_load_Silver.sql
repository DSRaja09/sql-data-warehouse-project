/* ============================================================
    The procedure Silver.load_Silver is responsible for loading clean, standardized data 
    from the Bronze layer into the Silver layer of your data warehouse.
    It Performs a full refresh ETL process with data cleaning, validation, and 
    performance timing for each table.
   ============================================================ */

CREATE OR ALTER PROCEDURE Silver.load_Silver AS
BEGIN
    BEGIN TRY

        DECLARE 
            @start_time        DATETIME,
            @end_time          DATETIME,
            @batch_start_time  DATETIME,
            @batch_end_time    DATETIME;

        SET @batch_start_time = GETDATE();



        /* ============================================================
           Load Customer Information
           - Removes duplicates using ROW_NUMBER()
           - Standardizes marital status and gender
           ============================================================ */

        PRINT '=== LOADING Silver.crm_cus_info from Bronze.crm_cus_info ===';

        TRUNCATE TABLE Silver.crm_cus_info;

        SET @start_time = GETDATE();

        INSERT INTO Silver.crm_cus_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gender,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname)  AS cst_lastname,

            CASE
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'N/A'
            END AS cst_marital_status,

            CASE
                WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
                WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
                ELSE 'N/A'
            END AS cst_gender,

            cst_create_date

        FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY cst_id
                        ORDER BY cst_create_date
                    ) AS Checker
                FROM Bronze.crm_cus_info
                WHERE cst_id IS NOT NULL
             ) t
        WHERE t.Checker = 1;

        SET @end_time = GETDATE();

        PRINT 'Time needed to Load: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);



        /* ============================================================
           Load Product Information
           - Extracts category and product keys
           - Calculates end date using LEAD() for SCD logic
           ============================================================ */

        PRINT '=== LOADING Silver.crm_prd_info from Bronze.crm_prd_info ===';

        TRUNCATE TABLE Silver.crm_prd_info;

        SET @start_time = GETDATE();

        INSERT INTO Silver.crm_prd_info (
            prd_id,
            prd_cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
            prd_id,

            REPLACE(
                SUBSTRING(prd_key, 1, 5),
                '-',
                '_'
            ) AS prd_cat_id,

            SUBSTRING(
                prd_key,
                7,
                LEN(prd_key)
            ) AS prd_key,

            prd_nm,

            ISNULL(prd_cost, 0) AS prd_cost,

            CASE UPPER(prd_line)
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'M' THEN 'Mountain'
                WHEN 'T' THEN 'Touring'
                ELSE 'N/A'
            END AS prd_line,

            CAST(prd_start_dt AS DATE) AS prd_start_dt,

            -- Determine end date using next start date
            CAST(
                LEAD(prd_start_dt) OVER (
                    PARTITION BY prd_key
                    ORDER BY prd_start_dt
                ) - 1
            AS DATE) AS prd_end_dt

        FROM Bronze.crm_prd_info;

        SET @end_time = GETDATE();

        PRINT 'Time needed to Load: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);



        /* ============================================================
           Load Sales Details
           - Validates dates
           - Recalculates incorrect sales and price values
           ============================================================ */

        PRINT '=== LOADING Silver.crm_sales_details from Bronze.crm_sales_details ===';

        TRUNCATE TABLE Silver.crm_sales_details;

        SET @start_time = GETDATE();

        INSERT INTO Silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,

            -- Validate order date
            CASE
                WHEN sls_order_dt <= 0
                     OR LEN(sls_order_dt) != 8
                THEN NULL
                ELSE CAST(
                        CAST(sls_order_dt AS VARCHAR)
                     AS DATE)
            END AS sls_order_dt,

            -- Validate ship date
            CASE
                WHEN sls_ship_dt <= 0
                     OR LEN(sls_ship_dt) != 8
                THEN NULL
                ELSE CAST(
                        CAST(sls_ship_dt AS VARCHAR)
                     AS DATE)
            END AS sls_ship_dt,

            -- Validate due date
            CASE
                WHEN sls_due_dt <= 0
                     OR LEN(sls_due_dt) != 8
                THEN NULL
                ELSE CAST(
                        CAST(sls_due_dt AS VARCHAR)
                     AS DATE)
            END AS sls_due_dt,

            -- Fix incorrect sales values
            CASE
                WHEN sls_sales IS NULL
                     OR sls_sales <= 0
                     OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * CAST(ABS(sls_price) AS INT)
                ELSE sls_sales
            END AS sls_sales,

            sls_quantity,

            -- Fix incorrect price values
            CASE
                WHEN sls_price IS NULL
                     OR sls_price <= 0
                     OR sls_price != NULLIF(sls_sales, 0) / sls_quantity
                THEN NULLIF(sls_sales, 0) / sls_quantity
                ELSE sls_price
            END AS sls_price

        FROM Bronze.crm_sales_details;

        SET @end_time = GETDATE();

        PRINT 'Time needed to Load: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);



        /* ============================================================
           Load ERP Customer Data
           ============================================================ */

        PRINT '=== LOADING Silver.erp_cust_az12 from Bronze.erp_cust_az12 ===';

        TRUNCATE TABLE Silver.erp_cust_az12;

        SET @start_time = GETDATE();

        INSERT INTO Silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT
            CASE
                WHEN cid LIKE 'NAS%'
                THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END AS cid,

            CASE
                WHEN bdate > GETDATE()
                THEN NULL
                ELSE bdate
            END AS bdate,

            CASE
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE')
                THEN 'Male'
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE')
                THEN 'Female'
                ELSE 'N/A'
            END AS gen

        FROM Bronze.erp_cust_az12;

        SET @end_time = GETDATE();

        PRINT 'Time needed to Load: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);



        /* ============================================================
           Load Location Data
           ============================================================ */

        PRINT '=== LOADING Silver.erp_loc_a101 from Bronze.erp_loc_a101 ===';

        TRUNCATE TABLE Silver.erp_loc_a101;

        SET @start_time = GETDATE();

        INSERT INTO Silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT
            REPLACE(cid, '-', '') AS cid,

            CASE
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) IS NULL
                     OR cntry = ''
                THEN 'N/A'
                ELSE TRIM(cntry)
            END AS cntry

        FROM Bronze.erp_loc_a101;

        SET @end_time = GETDATE();

        PRINT 'Time needed to Load: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);



        /* ============================================================
           Load Product Category Data
           ============================================================ */

        PRINT '=== LOADING Silver.erp_px_cat_g1v2 from Bronze.erp_px_cat_g1v2 ===';

        TRUNCATE TABLE Silver.erp_px_cat_g1v2;

        SET @start_time = GETDATE();

        INSERT INTO Silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintainace
        )
        SELECT
            id,
            cat,
            subcat,
            maintainace
        FROM Bronze.erp_px_cat_g1v2;

        SET @end_time = GETDATE();

        PRINT 'Time needed to Load: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);



        SET @batch_end_time = GETDATE();

        PRINT 'Total Batch Time: '
            + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR);



    END TRY

    BEGIN CATCH

        PRINT 'ERROR OCCURRED DURING SILVER LOAD';

        PRINT 'Error Message: '
            + ERROR_MESSAGE();

        PRINT 'Error Number: '
            + CAST(ERROR_NUMBER() AS NVARCHAR);

        PRINT 'Error Line: '
            + CAST(ERROR_LINE() AS NVARCHAR);

    END CATCH

END;
