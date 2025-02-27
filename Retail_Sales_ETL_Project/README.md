1. Project Overview

This project demonstrates an end-to-end ETL (Extract, Transform, Load) process using real-world retail sales and inventory data. The goal is to extract raw data from AWS S3, process it in Snowflake using SQL, and visualize insights in Tableau.

Key Steps:

Extract: Loaded raw retail_sales_dataset.csv data into an AWS S3 bucket.

Pipeline: Used Fivetran to extract data into Snowflake.

Transform: Cleaned and transformed data in Snowflake using SQL queries.

Prepare for Visualization: Optimized data for Tableau.

Load & Visualize: Created dashboards and published them on Tableau Public.

2. Technologies Used

AWS S3 - Cloud storage for raw data

Fivetran - ETL pipeline automation

Snowflake - Cloud data warehouse

SQL - Data cleaning and transformation

Tableau - Data visualization

VSCode - Development environment

3. Repository Structure

Retail_Sales_ETL_Project/
│-- data/                      # Sample dataset (if applicable)
│-- sql_queries/               # SQL transformation scripts
│-- notebooks/                 # word or pdf files (if used)
│-- tableau/                   # Tableau dashboards & visualizations
│-- images/                    # Screenshots of dashboards
│-- README.md                  # Project documentation

4. SQL Transformation Steps

Here are key SQL queries used in the transformation process:

Cleaning Data:Standardize formats, Removing duplicates and null values

Data Transformation Steps: 
1.Aggregate Sales & Customer Data for Reporting:
Total Sales by Product Category
a.Summarize total revenue and quantity sold per category
2.Sales Trend Over Time:
a.Track revenue and quantity sold over time
3.Customer Demographics Analysis
a.Sales breakdown by Gender & Age Group
4.Create Final Reporting Table for Tableau:
a.Join all transformed tables into a final dataset

5. Tableau Dashboard

Link to Tableau Public Dashboard: View Dashboard

Includes visualizations for:

a.Sales trends over time
b.Sales by gender
c.Revenue by category
d.Ccustomer demography

6. How to Reproduce

Clone this repository:

git clone https://github.com/toppizzle/PORTFOLIO_PROJECTS.git

Navigate to the project folder:

cd Portfolio_Projects/Retail_Sales_ETL_Project

Explore SQL queries in sql_queries/

Open and run the Tableau dashboard

7. Future Improvements

Automate the ETL process with Apache Airflow

Integrate PySpark for handling larger datasets

Implement Power BI dashboards as an alternative

8. Contact

Feel free to connect with me:

GitHub: Your GitHub Profile

LinkedIn: Your LinkedIn Profile

