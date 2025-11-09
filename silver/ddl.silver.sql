--silver LAYER---

IF OBJECT_ID ('silver.crm_cust_info', 'U') IS NOT NULL
     DROP TABLE silver.crm_cust_info

CREATE TABLE silver.crm_cust_info(
cst_id INT,
cst_key NVARCHAR (50),
cst_firstname NVARCHAR (50),
cst_lastname NVARCHAR (50),
cst_marital_status NVARCHAR (50),
cst_gndr VARCHAR (50),
cst_create_date DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
     DROP TABLE silver.crm_prd_info

CREATE TABLE silver.crm_prd_info(
prd_id INT,
prd_key NVARCHAR (50),
prd_nm NVARCHAR (50),
prd_cost INT,
prd_line NVARCHAR (50),
prd_start_dt DATE,
prd_end_dt DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL
     DROP TABLE silver.crm_sales_details

CREATE TABLE silver.crm_sales_details(
sls_ord_num NVARCHAR (50),
sls_prd_key NVARCHAR (50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID ('silver.erp_CUST_AZ12', 'U') IS NOT NULL
     DROP TABLE silver.erp_CUST_AZ12

CREATE TABLE silver.erp_CUST_AZ12(
CID NVARCHAR (50),
BDATE DATE,
GEN NVARCHAR (50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID ('silver.erp_LOC_A101', 'U') IS NOT NULL
     DROP TABLE silver.erp_LOC_A101

CREATE TABLE silver.erp_LOC_A101(
CID NVARCHAR (50),
CNTRY NVARCHAR (50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID ('silver.erp_PX_CAT_G1V2', 'U') IS NOT NULL
     DROP TABLE silver.erp_PX_CAT_G1V2

CREATE TABLE silver.erp_PX_CAT_G1V2(
ID NVARCHAR (50),
CAT NVARCHAR (50),
SUBCAT NVARCHAR (50),
MAINTENANCE NVARCHAR (50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);


---------------------------------------------------------------------------------------------------------------------------
------------STORED PROCEDURE 
---------------------------------------------------------------------------------------------------------------------------
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

    ---DEVELOPING SQL LOAD SCRIPTS BULK INSERT--

    SET @start_time = GETDATE();

    PRINT'>>>Trunacating Table:silver.crm_cust_info>>>'
    TRUNCATE TABLE silver.crm_cust_info --EMPTYING THE TABLE--

    PRINT'>>>Inserting Data Into:silver.crm_cust_infos>>>'
    BULK INSERT silver.crm_cust_info
    FROM 'C:\Users\duwar\OneDrive\Documents\000000000000sql\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH(
        FIRSTROW=2, --CAZ 1ST ROW HAS COULMN NAMES YEH--
        FIELDTERMINATOR =',', --WHAT DIVIDES THE DATA INTO SEPARATE COULMNS--
        TABLOCK --LOCKING THE TABLE--
    );

    SET @end_time = GETDATE();
    PRINT'>>LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT'--------'

    ---------------------------------------------------------
    SET @start_time = GETDATE();

    PRINT'>>>Trunacating Table:silver.crm_prd_info>>>'
    TRUNCATE TABLE silver.crm_prd_info --EMPTYING THE TABLE--

    PRINT'>>>Inserting Data Into:silver.crm_prd_infos>>>'
    BULK INSERT silver.crm_prd_info
    FROM 'C:\Users\duwar\OneDrive\Documents\000000000000sql\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH(
        FIRSTROW=2, --CAZ 1ST ROW HAS COULMN NAMES YEH--
        FIELDTERMINATOR =',', --WHAT DIVIDES THE DATA INTO SEPARATE COULMNS--
        TABLOCK --LOCKING THE TABLE--
    );

    SET @end_time = GETDATE();
    PRINT'>>LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT'--------'

    --------------------------------------------------------
    SET @start_time = GETDATE();

    PRINT'>>>Trunacating Table:silver.crm_sales_details>>>'
    TRUNCATE TABLE silver.crm_sales_details --EMPTYING THE TABLE--

    PRINT'>>>Inserting Data Into:silver.crm_sales_details>>>'
    BULK INSERT silver.crm_sales_details
    FROM 'C:\Users\duwar\OneDrive\Documents\000000000000sql\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH(
        FIRSTROW=2, --CAZ 1ST ROW HAS COULMN NAMES YEH--
        FIELDTERMINATOR =',', --WHAT DIVIDES THE DATA INTO SEPARATE COULMNS--
        TABLOCK --LOCKING THE TABLE--
    );

    SET @end_time = GETDATE();
    PRINT'>>LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT'--------'

    -------------------------------------------------------------------------------

    PRINT'----------------------------------------------------------------------------'
    PRINT'LOADING ERP TABLES'
    PRINT'----------------------------------------------------------------------------'

    -------------------------------------------------------------------------------
    SET @start_time = GETDATE();

    PRINT'>>>Trunacating Table:silver.erp_CUST_AZ12>>>'
    TRUNCATE TABLE silver.erp_CUST_AZ12 --EMPTYING THE TABLE--

    PRINT'>>>Inserting Data Into:silver.erp_CUST_AZ12>>>'
    BULK INSERT silver.erp_CUST_AZ12
    FROM 'C:\Users\duwar\OneDrive\Documents\000000000000sql\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
    WITH(
        FIRSTROW=2, --CAZ 1ST ROW HAS COULMN NAMES YEH--
        FIELDTERMINATOR =',', --WHAT DIVIDES THE DATA INTO SEPARATE COULMNS--
        TABLOCK --LOCKING THE TABLE--
    );

    SET @end_time = GETDATE();
    PRINT'>>LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT'--------'

    ------------------------------------------------------------
    SET @start_time = GETDATE();

    PRINT'>>>Trunacating Table:silver.erp_LOC_A101>>>'
    TRUNCATE TABLE silver.erp_LOC_A101 --EMPTYING THE TABLE--

    PRINT'>>>Inserting Data Into:silver.erp_LOC_A101>>>'
    BULK INSERT silver.erp_LOC_A101
    FROM 'C:\Users\duwar\OneDrive\Documents\000000000000sql\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
    WITH(
        FIRSTROW=2, --CAZ 1ST ROW HAS COULMN NAMES YEH--
        FIELDTERMINATOR =',', --WHAT DIVIDES THE DATA INTO SEPARATE COULMNS--
        TABLOCK --LOCKING THE TABLE--
    );

    SET @end_time = GETDATE();
    PRINT'>>LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT'--------'

    ------------------------------------------------------------
    SET @start_time = GETDATE();

    PRINT'>>>Trunacating Table:silver.erp_PX_CAT_G1V2>>>'
    TRUNCATE TABLE silver.erp_PX_CAT_G1V2 --EMPTYING THE TABLE--

    PRINT'>>>Inserting Data Into:silver.erp_PX_CAT_G1V2>>>'
    BULK INSERT silver.erp_PX_CAT_G1V2
    FROM 'C:\Users\duwar\OneDrive\Documents\000000000000sql\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH(
        FIRSTROW=2, --CAZ 1ST ROW HAS COULMN NAMES YEH--
        FIELDTERMINATOR =',', --WHAT DIVIDES THE DATA INTO SEPARATE COULMNS--
        TABLOCK --LOCKING THE TABLE--
    );

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



-------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------

--EXECUTING THE STORED PROCEDURE OF THE silver LAYER

EXEC silver.load_silver

-------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------
