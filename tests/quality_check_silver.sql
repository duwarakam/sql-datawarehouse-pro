/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results

--=========================================================================--
--            CONDUCTING CHECKS CRM Table 1 - cust_info                    --
--=========================================================================--


--check nulls or duplicates in primary key
---expectation : no result

SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

--=======================================--
---check for unwanted spaces--
--=======================================--

--1.customer firstname check--

select cst_firstname 
from [silver].[crm_cust_info]
where cst_firstname  != TRIM(cst_lastname)


--2.customer gender check---
select cst_gndr
from [silver].[crm_cust_info]
where cst_gndr  != TRIM(cst_gndr)


--DATA STANDARDIZATION & CONSISTENCY--

SELECT DISTINCT cst_gndr
from [DataWareHouse].[silver].[crm_cust_info]

SELECT DISTINCT cst_marital_status
FROM [DataWareHouse].[silver].[crm_cust_info]

-------------------------------------------------====================================================================
  --CHECKS FOR CRM TABLE 2 PRD INFO--
  --=====================================================================================================================
USE DataWareHouse;

--check nulls or duplicates in primary key
---expectation : no result

SELECT
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL


--prd_info has a matching cat_id with px_cat_g1v2 table as id (here we try to figure out what's missing out and bring them into the table)
SELECT
      [prd_id],
      [prd_key],
       REPLACE(SUBSTRING(prd_key, 1, 5), '-','_')AS cat_id,
       [prd_nm]
      ,[prd_cost]
      ,[prd_line]
      ,[prd_start_dt]
      ,[prd_end_dt]
  FROM [DataWareHouse].[bronze].[crm_prd_info]
  
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') NOT IN
(SELECT distinct id from bronze.erp_PX_CAT_G1V2)

--prd_key checking whether customers have products or not

SELECT
      [prd_id],
      [prd_key],
       REPLACE(SUBSTRING(prd_key, 1, 5), '-','_')AS cat_id,
       SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
       [prd_nm]
      ,[prd_cost]
      ,[prd_line]
      ,[prd_start_dt]
      ,[prd_end_dt]
  FROM [DataWareHouse].[bronze].[crm_prd_info]

where SUBSTRING(prd_key, 7, LEN(prd_key))not  in
(SELECT sls_prd_key FROM bronze.crm_sales_details)

--=======================================--
---check for unwanted spaces--
--=======================================--

--1.product namecheck--

select prd_nm 
from [bronze].[crm_prd_info]
where prd_nm  != TRIM(prd_nm)

--=======================================--
---checks for nulls / negatives--
--=======================================--
SELECT prd_cost
from bronze.crm_prd_info
where prd_cost<0 OR prd_cost IS NULL

--=======================================--
---checks for possible values in prd_line
--DATA STANDARDIZATION & CONSISTENCY--
--=======================================--

SELECT DISTINCT prd_line
from [DataWareHouse].[bronze].[crm_prd_info]

--SOLUTION-- 
      CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain' --using upper incase lowercase values r there
           WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road' -- trim used to catch unwanted spaces
           WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales' --using upper incase lowercase values r there
           WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring' -- trim used to catch unwanted spaces
           ELSE 'N/A'
      END AS prd_line,

--CHECK FOR INVALID DATE ORDERS--
--end date shldnt be earlier than start date--
--also the end of one record should be start of nect record of the same product 
--- it cannot be before the end date of the previous product--
--THEREFORE END DATE = START DATE OF THE NEXT RECOERD__(AND SUBTRACT ONE DAY FROM IT TO MAKE IT LOOK CLEAN-

SELECT*
FROM bronze.crm_prd_info
where prd_end_dt < prd_start_dt

--SOLUTION--

   CAST(prd_start_dt AS DATE) AS prd_start_dt,
      --LEAD() access values from the nest row within a window
      CAST(
    DATEADD(DAY, -1,  --- this part is here because if we use -1 normally the operand clashes so here dateadd is used 
    --IT IS KNOWN AS DATA ENRICHMENT - ADDING A NEW VALUE TO THE DATA - ENHANCING FOR ANALYSIS
        LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
    ) AS DATE
) AS prd_end_dt
  FROM [DataWareHouse].[bronze].[crm_prd_info]

--------------------------------------------


--========================================================--
--CONDUCTING CHECKS FOR CRM TABLE 3 - SALES DETAILS
--========================================================--

--CHECKING IF FOREIGN KEYS WORK--
SELECT 
       [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      ,[sls_order_dt],
      ,[sls_ship_dt]
      ,[sls_due_dt]
      ,[sls_sales]
      ,[sls_quantity]
      ,[sls_price]
      ,[dwh_create_date]
  FROM [DataWareHouse].[silver].[crm_sales_details]
  WHERE sls_cust_id NOT IN (SELECT prd_key FROM silver.crm_prd_info)

  --CHECK FOR INVALID DATES FOR ORDER,SHIP AND DUE DATES
  select
  NULLIF(sls_order_dt,0)
  from bronze.crm_sales_details
  where sls_order_dt <=0 or len(sls_order_dt) !=8 OR  sls_order_dt >20500101 OR sls_order_dt <19000101

  --check invalid date orders--
  SELECT *
  FROM bronze.crm_sales_details
  WHERE sls_order_dt>sls_ship_dt or sls_order_dt> sls_due_dt
  --ship date order date shouldnt be greater than ship or due date

  -----business rules check

 -- sales=quantity *price
 -- no negatives, 0, nulls allowed

 SELECT distinct --distinct removes duplictes
 sls_sales,
 sls_quantity,
 sls_price
 FROM bronze.crm_sales_details
 WHERE sls_sales is null or sls_quantity is null or sls_price is null
 or sls_sales <=0 or sls_quantity<=0 or sls_price<=0
 order by sls_sales, sls_quantity, sls_price

 ---SOLUTION FOR ABOVE---

SELECT DISTINCT  -- DISTINCT removes duplicates
    sls_sales AS old_sls_sales,
    sls_quantity,
    sls_price AS old_sls_price,

    CASE
        WHEN sls_sales IS NULL 
          OR sls_sales <= 0  
          OR sls_sales != sls_quantity * ABS(sls_price)  -- ABS returns absolute value
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,

    CASE
        WHEN sls_price IS NULL 
          OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price

FROM bronze.crm_sales_details


-------------------------------------------------====================================================================
  --CHECKS FOR ERP TABLE 1 CUST AZ12--
  --=====================================================================================================================
USE DataWareHouse;

--================================================================--
-- CID OF the erp table has NAS for some of those and this code demonstartes and cleaner version when compared to crm table cust_info 
--ensuring both of the CID & cst_id Match--
-=================================================================--

SELECT
       CID,
       CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
        ELSE CID
       END AS CID
      ,[BDATE]
      ,[GEN]
  FROM [DataWareHouse].[bronze].[erp_CUST_AZ12]
  WHERE CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
        ELSE CID
       END NOT IN (SELECT DISTINCT cst_key from silver.crm_cust_info) -- to check the transformatation works or not


--============================
--checking date 
--===========================

SELECT
CID,
      [BDATE]
      FROM [DataWareHouse].[bronze].[erp_CUST_AZ12]
      where BDATE < '1924-01-01' OR BDATE > GETDATE()

---YOU NEED TO REPORT THEM TO SOURCE SYSTEM OR REPLACE WITH NULL


--SOLUTION--

CASE WHEN BDATE > GETDATE() THEN NULL
ELSE BDATE
END AS BDATE

-- CHECKING POSSIBLE VALUES OF GEN-- 
--DATA STANDARDIZATION & CONSISTENCY--

SELECT DISTINCT GEN
FROM bronze.erp_CUST_AZ12

---SOLUTION
CASE WHEN UPPER(TRIM(GEN)) IN ('F', 'Female') THEN 'Female'
     WHEN UPPER(TRIM(GEN)) IN ('M', 'Male' ) THEN 'Male'
     ELSE N/A
END GEN 

--========================================================--
--CONDUCTING CHECKS FOR ERP TABLE 2 - LOCA101
--========================================================--
USE DataWareHouse;

SELECT
CID,
CNTRY
FROM bronze.erp_LOC_A101 WHERE REPLACE (CID, '-', '') NOT IN
(SELECT CST_KEY FROM bronze.crm_cust_info)


-- cid from loc table has a '-' so gotta remove it so it matches with cst_key from cust_info table--

WHERE REPLACE (CID, '-', '') NOT IN


--DATA STANDARDIZATION & CONSISTENCY--
SELECT DISTINCT CNTRY
FROM bronze.erp_LOC_A101
ORDER BY CNTRY

CASE WHEN TRIM(CNTRY) = 'DE' THEN 'GERMANY'
	WHEN TRIM(CNTRY) = IN ('US', 'USA') THEN 'United States'
	WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'N/A'
	ELSE TRIM(CNTRY) 
END AS CNTRY

--========================================================--
--CONDUCTING CHECKS FOR ERP TABLE 3 - PX_CAT_G1V2
--========================================================--
USE DataWareHouse;

SELECT
ID,
CAT,
SUBCAT,
MAINTENANCE
from bronze.erp_PX_CAT_G1V2

--check for unwanted spaces--
select * from bronze.erp_PX_CAT_G1V2
WHERE CAT!=TRIM(CAT) OR SUBCAT!=TRIM(SUBCAT) OR MAINTENANCE!=TRIM(MAINTENANCE) 

-- DATA STANDARDIZATION & CONSISTENCY
SELECT DISTINCT
CAT
FROM bronze.erp_PX_CAT_G1V2
--NO ISSUES

SELECT DISTINCT
SUBCAT
FROM bronze.erp_PX_CAT_G1V2
--NO ISSUES

SELECT DISTINCT
MAINTENANCE
FROM bronze.erp_PX_CAT_G1V2


--================GOOD NO ISSUES================


