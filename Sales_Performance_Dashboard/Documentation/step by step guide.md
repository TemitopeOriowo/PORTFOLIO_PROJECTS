# Step-by-Step Guide: Sales Performance Dashboard in Power BI

## Step 1: Define the Dashboard Objectives
The Sales Performance Dashboard provides key insights into:
- **Overall sales performance** (total revenue, trends, profitability)
- **Sales agent performance** (who's selling the most?)
- **Product insights** (top-selling products & revenue distribution)
- **Customer behavior** (repeat customers, average order value)
- **Regional performance** (sales by region)

## Step 2: Prepare Your Dataset
The dataset includes key entities:
- **Orders**: Order ID, Date, Product, Customer, Quantity, Sales, Profit
- **Employees**: Employee Name, Role, Sales Contribution, Region
- **Customers**: Customer Name, Purchase Frequency, Segmentation
- **Products**: Category, Sub-Category, Sales, Profit

## Step 3: Data Cleaning & Transformation
Use **Power Query** to clean and transform the dataset:
1. Remove duplicates
2. Format dates correctly
3. Ensure proper relationships between tables
4. Handle missing values

## Step 4: Create DAX Measures for Key Metrics
Use the following **DAX formulas** for key metrics:
- **Total Sales**
- **Total Profit**
- **Profit Margin (%)**
- **Average Order Value**
- **Loyalty Rate (Repeat Customers)**
- **Most Popular Product (By Sales)**

## Step 5: Build the Dashboard Visualizations
The dashboard includes the following visuals:
- **Card Visuals**: Total Sales, Total Profit, Profit Margin, Average Order Value, Loyalty Rate, Most Popular Product
- **Table**: Sales Performance of Each Sales Agent
- **Line Chart**: Sales Trend Over Time
- **Doughnut Chart**: Revenue Share by Product Category
- **Bar Chart**: Sales by Sales Agent

## Step 6: Add Filters & Slicers
Enhance interactivity with these slicers:
- **Date Range Slicer** (Select sales data for specific periods)
- **Employee Name Slicer** (Filter performance by individual sales agents)
- **Product Slicer** (Analyze sales for different products)
- **Region Slicer** (View performance across different locations)

## Step 7: Save & Export
- Save the final `Sales_Performance_Dashboard.pbix` file
- Export dashboard snapshots for GitHub documentation
