--======================================================================================--
--STARTING OFF GOLD LAYER (MAKE SURE TO TAKE CLEANED DATA FROM SILVER LAYER--
--=====================================================================================--
--=============================
--QUALITY CHECK FOR GOLD LAYER--
--CREATES JOINS FOR TABLESRELATED TO CUSTOMER INFORMATION- 

USE DataWareHouse;
SELECT cst_id, COUNT(*) FROM

    (SELECT
        ci.cst_id,
        ci.cst_key,
        ci.cst_firstname,
        ci.cst_lastname,
        ci.cst_marital_status,
        ci.cst_gndr,
        ci.cst_create_date,
        ca.bdate,
        ca.gen,
        la.cntry
FROM silver.crm_CUST_INFO as ci
LEFT JOIN silver.erp_CUST_AZ12 as ca
    ON  ci.cst_key = ca.CID
LEFT JOIN silver.erp_LOC_A101 AS la
    on  ci.cst_key =la.CID
    )t 
    GROUP BY cst_id HAVING COUNT(*) > 1

    --===========================
   -- ABOVE CHECKS IF ANY DUPLICATES ARE PRESENT WHILE JOINING TABLES--
   --===========================

--========================================
-- so when you join tables sometimes u get null values even if u replaced w n/a 
--it happens becaz u join tables and chances are there to have extras (nulls often come from joined tables 
--if sql finds no match when u join tables it will appear as NULL
--================================================================
---SO MASTER SOURCE IS CRM WHICH EANS YOU PRIORATIZE DATA FROM CRM CUST INFO TABLE AND CHANGE OTHERS ACCORDINGLY--
------------------------------------------------------------------------------------------------------------------
--BELOW CODE TO CHECK--
SELECT
        ca.gen,
        ci.cst_gndr
FROM silver.crm_CUST_INFO as ci
LEFT JOIN silver.erp_CUST_AZ12 as ca
    ON  ci.cst_key = ca.CID
LEFT JOIN silver.erp_LOC_A101 AS la
    on  ci.cst_key =la.CID

--SOLUTON-
SELECT DISTINCT
        ca.GEN,
        ci.cst_gndr,
        
        CASE 
        WHEN ci.cst_gndr = 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
        ELSE COALESCE(ca.gen, 'n/a')  			   -- Fallback to ERP data
    END  AS new_gen
FROM silver.crm_CUST_INFO as ci
LEFT JOIN silver.erp_CUST_AZ12 as ca
    ON  ci.cst_key = ca.CID
LEFT JOIN silver.erp_LOC_A101 AS la
    on  ci.cst_key =la.CID
ORDER BY 1,2


--NAMING COULUMNS AGAIN ACCORDING TO GENERAL PRINCIPLES & ORDERING THE COULMNS--
SELECT
    ci.cst_id                          AS customer_id,
    ci.cst_key                         AS customer_number,
    ci.cst_firstname                   AS first_name,
    ci.cst_lastname                    AS last_name,
    la.cntry                           AS country,
    ci.cst_marital_status              AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
        ELSE COALESCE(ca.gen, 'n/a')  			   -- Fallback to ERP data
    END                                AS gender,
    ca.bdate                           AS birthdate,
    ci.cst_create_date                 AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;


--DIMENSION V/S FACT--
--DIMENSION IS - DESCRIPTIVE WHO WHAT WHERE
--THEREFORE IT'S DIEMENSION--
--BUT WE NEED A PRIMARY KEY WHICH IS NOT CST_ID INSTEAD CREATE ONE CALLED
--THE SURROGATE KEY (SYSTEM GENERATED)
--WE USE A WINDOW FUNCTION FOR THIS ROW_NUMBER)

SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
    ci.cst_id                          AS customer_id,
    ci.cst_key                         AS customer_number,
    ci.cst_firstname                   AS first_name,
    ci.cst_lastname                    AS last_name,
    la.cntry                           AS country,
    ci.cst_marital_status              AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
        ELSE COALESCE(ca.gen, 'n/a')  			   -- Fallback to ERP data
    END                                AS gender,
    ca.bdate                           AS birthdate,
    ci.cst_create_date                 AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;

 
 --CREATING OBJECTS (VIEWS)
 CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
    ci.cst_id                          AS customer_id,
    ci.cst_key                         AS customer_number,
    ci.cst_firstname                   AS first_name,
    ci.cst_lastname                    AS last_name,
    la.cntry                           AS country,
    ci.cst_marital_status              AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
        ELSE COALESCE(ca.gen, 'n/a')  			   -- Fallback to ERP data
    END                                AS gender,
    ca.bdate                           AS birthdate,
    ci.cst_create_date                 AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;
GO


--==========================================-
--JOINING PRODUCT TABLES
--==========================================


CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data
GO


--USING COUNT DUPLICATE RECORD CHECK CONDUCTED AND RETURNED SUCCESSFULY--
--ABOVE IS A DIMESNION WHO WHAT WHERE PRODUCT SO WE CREATE A SURROGATE KEY
--CREATING A VIEW FOR PRODUCTS DIMENSION

--CHECK FOR THE VIEW
SELECT * FROM gold.dim_products
SELECT * FROM gold.dim_customers



--====================================================
--creating sales table
--=====================================================

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
GO

--the above sales details is  a fact sincw qs like how much and how many are there
--a building fact here instead of using and creating a surragate key here use the dimensions surroate keys insread of ids--


--check for view
SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
on c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
on p.product_key = f.product_key
where c.customer_key is null 

--since above shows no results it means everything is matching well
--foreign key integrity data validation check
