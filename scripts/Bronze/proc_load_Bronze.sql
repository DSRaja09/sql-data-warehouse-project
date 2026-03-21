/*
===============================================================================
Procedure Name : Bronze.load_Bronze
Layer          : Bronze (Raw Data Layer)

Purpose:
This stored procedure loads raw data into the Bronze layer tables from
external CSV source files using BULK INSERT.

Process Overview:
1. Record batch start time.
2. For each source table:
   - Record table load start time
   - TRUNCATE existing data (full refresh strategy)
   - BULK INSERT new data from CSV
   - Record table load end time
   - Print load duration
3. Record total batch execution time.
4. Handle errors using TRY-CATCH.

Performance Features:
- TRUNCATE TABLE for fast data reset
- BULK INSERT for high-speed ingestion
- TABLOCK to optimize bulk loading
- Execution time tracking for monitoring

Execution Context:
Typically executed as the first step in an ETL / ELT pipeline
===============================================================================
*/

CREATE OR ALTER PROCEDURE Bronze.load_Bronze
AS
BEGIN

    BEGIN TRY

        -----------------------------------------------------------------------
        -- Variable Declarations
        -----------------------------------------------------------------------
        DECLARE 
            @start_time        DATETIME,
            @end_time          DATETIME,
            @batch_start_time  DATETIME,
            @batch_end_time    DATETIME;

        -----------------------------------------------------------------------
        -- Start Batch Timer
        -----------------------------------------------------------------------
        PRINT '=== Loading Bronze Layer ===';

        SET @batch_start_time = GETDATE();



        -----------------------------------------------------------------------
        -- Section 1: Load CRM Tables
        -----------------------------------------------------------------------
        PRINT '=== Loading CRM Tables ===';



        -----------------------------------------------------------------------
        -- Load CRM Customer Information
        -----------------------------------------------------------------------
        SET @start_time = GETDATE();

        TRUNCATE TABLE Bronze.crm_cus_info;

        BULK INSERT Bronze.crm_cus_info
        FROM 'C:\Users\HP\Documents\SQL\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,          -- Skip header row
            FIELDTERMINATOR = ',', -- CSV delimiter
            TABLOCK                -- Improve bulk load performance
        );

        SET @end_time = GETDATE();

        PRINT '>>> crm_cus_info load time: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
            + ' seconds';



        -----------------------------------------------------------------------
        -- Load CRM Product Information
        -----------------------------------------------------------------------
        SET @start_time = GETDATE();

        TRUNCATE TABLE Bronze.crm_prd_info;

        BULK INSERT Bronze.crm_prd_info
        FROM 'C:\Users\HP\Documents\SQL\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();

        PRINT '>>> crm_prd_info load time: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
            + ' seconds';



        -----------------------------------------------------------------------
        -- Load CRM Sales Details
        -----------------------------------------------------------------------
        SET @start_time = GETDATE();

        TRUNCATE TABLE Bronze.crm_sales_details;

        BULK INSERT Bronze.crm_sales_details
        FROM 'C:\Users\HP\Documents\SQL\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();

        PRINT '>>> crm_sales_details load time: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
            + ' seconds';



        -----------------------------------------------------------------------
        -- Section 2: Load ERP Tables
        -----------------------------------------------------------------------
        PRINT '=== Loading ERP Tables ===';



        -----------------------------------------------------------------------
        -- Load ERP Customer Data
        -----------------------------------------------------------------------
        SET @start_time = GETDATE();

        TRUNCATE TABLE Bronze.erp_cust_az12;

        BULK INSERT Bronze.erp_cust_az12
        FROM 'C:\Users\HP\Documents\SQL\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();

        PRINT '>>> erp_cust_az12 load time: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
            + ' seconds';



        -----------------------------------------------------------------------
        -- Load ERP Location Data
        -----------------------------------------------------------------------
        SET @start_time = GETDATE();

        TRUNCATE TABLE Bronze.erp_loc_a101;

        BULK INSERT Bronze.erp_loc_a101
        FROM 'C:\Users\HP\Documents\SQL\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();

        PRINT '>>> erp_loc_a101 load time: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
            + ' seconds';



        -----------------------------------------------------------------------
        -- Load ERP Product Category Data
        -----------------------------------------------------------------------
        SET @start_time = GETDATE();

        TRUNCATE TABLE Bronze.erp_px_cat_g1v2;

        BULK INSERT Bronze.erp_px_cat_g1v2
        FROM 'C:\Users\HP\Documents\SQL\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();

        PRINT '>>> erp_px_cat_g1v2 load time: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
            + ' seconds';



        -----------------------------------------------------------------------
        -- End Batch Timer
        -----------------------------------------------------------------------
        SET @batch_end_time = GETDATE();

        PRINT '>>> Total Bronze load time: '
            + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR)
            + ' seconds';

        PRINT '=== Loading of Bronze Layer Completed ===';



    END TRY

    ---------------------------------------------------------------------------
    -- Error Handling
    ---------------------------------------------------------------------------
    BEGIN CATCH

        PRINT 'ERROR OCCURRED DURING BRONZE LOAD';

        PRINT 'Error Message: '
            + ERROR_MESSAGE();

        PRINT 'Error Number: '
            + CAST(ERROR_NUMBER() AS NVARCHAR);

        PRINT 'Error Line: '
            + CAST(ERROR_LINE() AS NVARCHAR);

    END CATCH

END;
