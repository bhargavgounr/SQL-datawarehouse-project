--Creating dimension tables(customers) like conneting customer information.
SELECT * FROM silver.crm_cust_info 
SELECT * FROM silver.erp_cust_az12
--Creating customer dimension table.
--THese tables ConneCted witH Cst_key and Cid and we require only few tables.

CREATE VIEW gold.dim_customers AS
SELECT
ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
ci.cst_id				AS customer_id,
ci.cst_key				AS customer_number,
ci.cst_firstname		AS first_name,
ci.cst_lastname			AS last_name,
la.cntry				AS country,
ci.cst_marital_status	AS marital_status,
ca.bdate				AS birthdate,
ci.cst_create_date		AS create_date,
CASE
	WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
	ELSE COALESCE(ca.gen, 'n/a')
END AS Gender
FROM silver.crm_cust_info as Ci
LEFT JOIN silver.erp_cust_az12 as Ca
on ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 as la
on ci.cst_key = la.cid;

--We have inserted above query in views to get the that data immediately.

SELECT * FROM gold.dim_customers;

--Creating dimension tables(prodcuts) like conneting customer information.
--both tables are connected with & joining both tables and get one.
SELECT *  FROM silver.crm_prd_info
SELECT * FROM silver.erp_px_cat_g1v2

CREATE VIEW gold.dim_products AS
SELECT
ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, 
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
FROM silver.crm_prd_info as pn
left join silver.erp_px_cat_g1v2 as pc
on pn.cat_id = pc.id
WHERE prd_end_dt is NULL;

--We have inserted above query in views to get the that data immediately.

SELECT * FROM gold.dim_products;

--Creating Fact Table with crm_sales_details & crm_prd_info, crm_cust_info
--Below are the three tables that we required.

SELECT * FROM silver.crm_sales_details;
SELECT * FROM silver.crm_prd_info;
SELECT * FROM silver.crm_cust_info

--creating fact table

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

	SELECT * FROM gold.fact_sales
