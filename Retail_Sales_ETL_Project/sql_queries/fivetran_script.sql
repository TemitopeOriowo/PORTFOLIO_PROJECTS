--created warehouse that fivetran will connect to.
create or replace warehouse fivetran_wh
warehouse_sizealter = xsmall;

--created database that fivetran will connect to.
create or replace database fivetran_db;

--created a schema for the target table in snowflake
create or replace schema fivetran_schema;

create role for fivetran
create or replace role fivetran_role;

--grant permission for the roles to use
--warehouse,database and create schema.
grant usage on warehouse fivetran_wh to role fivetran_role;
grant usage on database fivetran_db to role fivetran_role;
grant usage, create table on schema fivetran_db.fivetran_schema to role fivetran_role;
grant role fivetran_role to user oriowo;

--create target table to ingest data into snowflake
--from s3 bucket.
create table fivetran_schema.retail_sales_dataset
(
    Transaction_ID varchar not null,
    Date date not null,
    Customer_ID varchar not null,
    Gender varchar not null,
    Age int not null,
    Product_Category varchar not null,
    Quantity int not null,
    Price_per_Unit  float not null,
    Total_Amount float not null
);

--test to confirm data ingestion was received.
SELECT COUNT(*) FROM fivetran_schema.retail_sales_dataset;

--test to visualize 5 rolls of the data ingested.
select * from fivetran_schema.retail_sales_dataset limit 5;

SHOW TABLES;

--DATA TRANSFORMATION
--step 1:Create a Cleaned Version of retail_sales_dataset
--Standardize formats, remove duplicates, and handle NULLs
CREATE OR REPLACE TABLE retail_sales_cleaned AS 
SELECT  
    DISTINCT Transaction_ID,
    TRY_CAST(Date AS DATE) AS Transaction_Date,
    UPPER(Customer_ID) AS Customer_ID,
    UPPER(Gender) AS Gender,
    Age,
    UPPER(Product_Category) AS Product_Category,
    COALESCE(Quantity, 0) AS Quantity,
    COALESCE(Price_per_Unit, 0) AS Price_per_Unit,
    COALESCE(Total_Amount, 0) AS Total_Amount
FROM retail_sales_dataset
WHERE Transaction_ID IS NOT NULL;

--Step 2: Aggregate Sales & Customer Data for Reporting
--Total Sales by Product Category
--a.Summarize total revenue and quantity sold per category
CREATE OR REPLACE TABLE total_sales AS
SELECT 
    Product_Category,
    SUM(Quantity) AS Total_Units_Sold,
    SUM(Total_Amount) AS Total_Revenue
FROM retail_sales_cleaned
GROUP BY Product_Category;

select * from total_sales;

--Sales Trend Over Time
--b.Track revenue and quantity sold over time
CREATE OR REPLACE TABLE sales_trends AS
SELECT 
    Transaction_Date,
    SUM(Quantity) AS Total_Units_Sold,
    SUM(Total_Amount) AS Total_Revenue
FROM retail_sales_cleaned
GROUP BY Transaction_Date
ORDER BY Transaction_Date;
select * from sales_trends;

--Customer Demographics Analysis
--c.Sales breakdown by Gender & Age Group
CREATE OR REPLACE TABLE customer_demographics AS
SELECT 
    Gender,
    CASE 
        WHEN Age BETWEEN 18 AND 24 THEN '18-24'
        WHEN Age BETWEEN 25 AND 34 THEN '25-34'
        WHEN Age BETWEEN 35 AND 44 THEN '35-44'
        WHEN Age BETWEEN 45 AND 54 THEN '45-54'
        ELSE '55+'
    END AS Age_Group,
    SUM(Quantity) AS Total_Units_Sold,
    SUM(Total_Amount) AS Total_Revenue
FROM retail_sales_cleaned
GROUP BY Gender, Age_Group;
select * from customer_demographics limit 5;

select * from customer_demographics;

--Step 3: Create Final Reporting Table for Tableau
--Join all transformed tables into a final dataset
CREATE OR REPLACE TABLE final_retail_sales AS
SELECT 
    c.*,
    t.Total_Units_Sold AS Category_Units_Sold,
    t.Total_Revenue AS Category_Revenue,
    d.Age_Group,
    d.Total_Units_Sold AS Age_Units_Sold,
    d.Total_Revenue AS Age_Revenue
FROM retail_sales_cleaned AS c
LEFT JOIN total_sales AS t 
    ON c.Product_Category = t.Product_Category
LEFT JOIN customer_demographics AS d 
    ON c.Gender = d.Gender 
    AND CASE 
        WHEN c.Age BETWEEN 18 AND 24 THEN '18-24'
        WHEN c.Age BETWEEN 25 AND 34 THEN '25-34'
        WHEN c.Age BETWEEN 35 AND 44 THEN '35-44'
        WHEN c.Age BETWEEN 45 AND 54 THEN '45-54'
        ELSE '55+'
    END = d.Age_Group;  -- Now both are STRING
select * from final_retail_sales limit 5;
