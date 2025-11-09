EXEC bronze.load_bronze
EXEC silver.load_silver

DROP PROCEDURE IF EXISTS silver.load_silver;
GO

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN

    
   -----batch start & end time calculates how long the procedure tok to perform---
  DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
  BEGIN TRY
    SET @batch_start_time = GETDATE()
    PRINT'============================================================================'
    PRINT'LOADING silver LAYER'
    PRINT'============================================================================'

    PRINT'----------------------------------------------------------------------------'
    PRINT'LOADING CRM TABLES'
    PRINT'----------------------------------------------------------------------------'   
    --=============================================--
    --ALL SIX TABLES INSERTED FROM BRONZE TO SILVER--
    --=============================================--

    --===============================================
    --CRM TABLE 1 CUST INFO--
    --===============================================
    SET @start_time = GETDATE();
    PRINT '>>TRUNCATING TABLE silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info
    PRINT '>>INSERTING DATA INTO silver.crm_cust_info'

    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date)

    SELECT 
          cst_id,
          [cst_key]
          ,TRIM(cst_firstname) AS cst_firstname
          ,TRIM(cst_lastname) AS cst_lastname

          --SOLUTION FOR UNWATED SPACES--

          ,CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' --using upper incase lowercase values r there
               WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' -- trim used to catch unwanted spaces
               ELSE 'N/A'
          END cst_marital_status,
          CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' --using upper incase lowercase values r there
               WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male' -- trim used to catch unwanted spaces
               ELSE 'N/A'
               --soultion for gender & marital status expansion of terms--
          END cst_gndr,
          [cst_create_date]

    ----Removing duplicate primary keys 

    FROM(
          SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last
        From [bronze].[crm_cust_info]
        )t 
        where flag_last = 1 -- select the most recent record per customer

                SET @end_time = GETDATE();
    PRINT'>>LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT'--------'

    --===============================================
    --CRM TABLE 2 PRD INFO--
    --===============================================
    SET @start_time = GETDATE();
    PRINT '>>TRUNCATING TABLE silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info
    PRINT '>>INSERTING DATA INTO silver.crm_prd_info'

    INSERT silver.crm_prd_info(
    prd_id ,
    cat_id,
    prd_key,
    prd_nm ,
    prd_cost ,
    prd_line,
    prd_start_dt,
    prd_end_dt
    )

    SELECT
          [prd_id],
           REPLACE(SUBSTRING(prd_key, 1, 5), '-','_')AS cat_id,
           SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
           [prd_nm]
          ,ISNULL(prd_cost, 0) AS prd_cost,
          CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain' --using upper incase lowercase values r there
               WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road' -- trim used to catch unwanted spaces
               WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales' --using upper incase lowercase values r there
               WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring' -- trim used to catch unwanted spaces
               ELSE 'N/A'
          END AS prd_line,
           CAST(prd_start_dt AS DATE) AS prd_start_dt,
          --LEAD() access values from the nest row within a window
          CAST(
        DATEADD(DAY, -1, --- this part is here because if we use -1 normally the operand clashes so here dateadd is used 
            LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
        ) AS DATE
    ) AS prd_end_dt
      FROM [DataWareHouse].[bronze].[crm_prd_info]

              SET @end_time = GETDATE();
    PRINT'>>LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT'--------'
 
     --===============================================
    --CRM TABLE 3 SALES DETAILS--
    --================================================
    SET @start_time = GETDATE();
    PRINT '>>TRUNCATING TABLE silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details
    PRINT '>>INSERTING DATA INTO silver.crm_sales_details'

    INSERT INTO silver.crm_sales_details (
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
           [sls_ord_num]
          ,[sls_prd_key]
          ,[sls_cust_id],
           CASE WHEN sls_order_dt = 0 or LEN(sls_order_dt) !=8 THEN NULL
                ELSE CAST( CAST(sls_order_dt AS VARCHAR) AS DATE) --INT TO DATE IS NOT POOSIBLE SO HERE WE CAST AS VARCHAR AND THEN AS DATE
           END AS sls_order_dt
          ,CASE WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) !=8 THEN NULL
                ELSE CAST( CAST(sls_ship_dt AS VARCHAR) AS DATE) --INT TO DATE IS NOT POOSIBLE SO HERE WE CAST AS VARCHAR AND THEN AS DATE
           END AS sls_ship_dt
          ,CASE WHEN sls_due_dt = 0 or LEN(sls_due_dt) !=8 THEN NULL
                ELSE CAST( CAST(sls_due_dt AS VARCHAR) AS DATE) --INT TO DATE IS NOT POOSIBLE SO HERE WE CAST AS VARCHAR AND THEN AS DATE
           END AS sls_due_dt,
            CASE
            WHEN sls_sales IS NULL 
              OR sls_sales <= 0  
              OR sls_sales != sls_quantity * ABS(sls_price)  -- ABS returns absolute value
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
          [sls_quantity],
          CASE
            WHEN sls_price IS NULL 
              OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price
    FROM bronze.crm_sales_details

            SET @end_time = GETDATE();
    PRINT'>>LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT'--------'

    -------------------------------------------------------------------------------

    PRINT'----------------------------------------------------------------------------'
    PRINT'LOADING ERP TABLES'
    PRINT'----------------------------------------------------------------------------'

    -------------------------------------------------------------------------------
    SET @start_time = GETDATE();
     --===============================================
    --ERP TABLE 1 CUST_AZ12--
    --================================================
    SET @start_time = GETDATE();
    PRINT '>>TRUNCATING TABLE silver.erp_CUST_AZ12';
    TRUNCATE TABLE silver.erp_CUST_AZ12
    PRINT '>>INSERTING DATA INTO silver.erp_CUST_AZ12'
    INSERT INTO silver.erp_CUST_AZ12 (CID, BDATE, GEN)
    SELECT 
           CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
            ELSE CID
           END AS CID
          ,CASE WHEN BDATE > GETDATE() THEN NULL
                ELSE BDATE
           END AS BDATE
          ,CASE WHEN UPPER(TRIM(GEN)) IN ('F', 'Female') THEN 'Female'
                WHEN UPPER(TRIM(GEN)) IN ('M', 'Male' ) THEN 'Male'
                ELSE 'N/A'
           END GEN 
      FROM [DataWareHouse].[bronze].[erp_CUST_AZ12]

              SET @end_time = GETDATE();
    PRINT'>>LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT'--------'

    --===============================================
    --ERP TABLE 2 LOC_A101--
    --================================================
    SET @start_time = GETDATE();
    PRINT '>>TRUNCATING TABLE silver.erp_LOC_A101';
    TRUNCATE TABLE silver.erp_LOC_A101
    PRINT '>>INSERTING DATA INTO silver.erp_LOC_A101'
    INSERT INTO silver.erp_LOC_A101 (CID, CNTRY)


    SELECT
    REPLACE (CID, '-', '') AS CID,
    CASE WHEN TRIM(CNTRY) = 'DE' THEN 'GERMANY'
	    WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
	    WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'N/A'
	    ELSE TRIM(CNTRY) 
    END AS CNTRY
    FROM bronze.erp_LOC_A101 

            SET @end_time = GETDATE();
    PRINT'>>LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT'--------'

    --===============================================
    --ERP TABLE 3 PX_CAT_G1V2--
    --================================================
    SET @start_time = GETDATE();
    PRINT '>>TRUNCATING TABLE silver.erp_PX_CAT_G1V2';
    TRUNCATE TABLE silver.erp_PX_CAT_G1V2
    PRINT '>>INSERTING DATA INTO silver.erp_PX_CAT_G1V2'
    INSERT INTO silver.erp_PX_CAT_G1V2 (ID, CAT, SUBCAT, MAINTENANCE)

    SELECT
    ID,
    CAT,
    SUBCAT,
    MAINTENANCE
    FROM bronze.erp_PX_CAT_G1V2

        SET @end_time = GETDATE();
    PRINT'>>LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT'--------'

    ------------------------------------------------------------

    SET @batch_end_time=GETDATE();
        PRINT'=============================================================='
        PRINT'LOADING COMPLETED AT silver LAYER'
        PRINT'total duration: ' + CAST (DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
        PRINT'=============================================================='

        -----batch start & end time calculates how long the procedure tok to perform---

      END TRY
  BEGIN CATCH
        PRINT'=============================================================='
        PRINT'ERROR OCURRED DURING silver LAYER'
        PRINT'ERROR MESSAGE' + ERROR_MESSAGE();
        PRINT'ERROR MESSAGE' + CAST (ERROR_NUMBER() AS NVARCHAR);
        PRINT'ERROR MESSAGE' + CAST (ERROR_STATE() AS NVARCHAR);
        PRINT'=============================================================='
  END CATCH
END
