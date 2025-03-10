--CLEANING DATASET
-- ALL MY COLUMNS HAVE NOT NULL CONSTRAINT
-- SO THERE IS NO NULL VALUES
--BUT FOR SITUATIONS WHERE THERE ARE NULL CONTRAINT,
-- or nullable (i.e., they allow NULL values), and you want to identify 
--which nullable columns have NULL values and count them.
-- THIS DYNAMIC SQL QUERY WORKS AND SHOWS THE COUNT OF NULL COLUMNS.
DECLARE @SQL NVARCHAR(MAX) = '';  -- Variable to hold the dynamic SQL query

-- Loop through all nullable columns in the database
SELECT @SQL = @SQL + 
    'SELECT ''' + TABLE_NAME + ''' AS TableName, ''' + COLUMN_NAME + ''' AS ColumnName, COUNT(*) AS NullCount ' + 
    'FROM ' + QUOTENAME(TABLE_NAME) + ' ' +  -- Use QUOTENAME for safe handling of table names
    'WHERE ' + QUOTENAME(COLUMN_NAME) + ' IS NULL ' + 
    'HAVING COUNT(*) > 0 ' +  -- Only select where NULL values exist
    'UNION ALL ' 
FROM INFORMATION_SCHEMA.COLUMNS
WHERE IS_NULLABLE = 'YES'  -- Only check columns that allow NULLs

-- Remove the trailing 'UNION ALL'
SET @SQL = LEFT(@SQL, LEN(@SQL) - 10);

-- Print or execute the dynamic query
PRINT @SQL;  -- Uncomment to see the query
EXEC sp_executesql @SQL;  -- Executes the dynamic query

--CHECKING FOR DUPLICATES
-- THIS DYNAMIC QUERY LOOPS THROUGH THE TABLES AND RETURNS THE COUNT 
-- OF DUPLICATES FOUND IN A TABLE BY SPECIFYING BTHE COLUMNAME  AND THE VALUE

DECLARE @SQL NVARCHAR(MAX) = '';  -- Variable to hold the dynamic SQL query

-- Loop through all columns in the database
SELECT @SQL = @SQL + 
    'SELECT ''' + TABLE_NAME + ''' AS TableName, ''' + COLUMN_NAME + ''' AS ColumnName, ' + 
    'CAST(' + QUOTENAME(COLUMN_NAME) + ' AS VARCHAR(MAX)) AS Value, COUNT(*) AS DuplicateCount ' +  -- Cast to string to avoid type conversion errors
    'FROM ' + QUOTENAME(TABLE_NAME) + ' ' +  -- Use QUOTENAME for safe handling of table names
    'GROUP BY CAST(' + QUOTENAME(COLUMN_NAME) + ' AS VARCHAR(MAX)) ' +  -- Group by the string-casted column value
    'HAVING COUNT(*) > 1 ' +  -- Only show values that appear more than once (duplicates)
    'UNION ALL ' 
FROM INFORMATION_SCHEMA.COLUMNS
WHERE IS_NULLABLE = 'YES'  -- Only check nullable columns, or remove this if you want to check all columns

-- Remove the trailing 'UNION ALL'
SET @SQL = LEFT(@SQL, LEN(@SQL) - 10);

-- Print or execute the dynamic query
PRINT @SQL;  -- Uncomment to see the query
EXEC sp_executesql @SQL;  -- Executes the dynamic query


--STANDARDIZNG DATA FORMATS
--CHECKING ALL TABLES TO HAVE A LOOK AT THE FORMATS

SELECT TOP 10 * FROM DIMDATE;

SELECT TOP 10 * FROM DimHospitals;

SELECT TOP 10 * FROM DimInsurance;

SELECT TOP 10 * FROM DimPatients;
-- DimPatients have names in PatientName COLUMN WITH NUMBERS INBETWEEN NAMES
--CREATE A VIEW DURING TRANSFORMATION TO CORRECT THIS BEFORE LOADING IN POWER BI

SELECT TOP 10 * FROM DimPhysicians;
-- DimPhysicians have names in PhysicianName COLUMN WITH NUMBERS INBETWEEN NAMES
--CREATE A VIEW DURING TRANSFORMATION TO CORRECT THIS BEFORE LOADING IN POWER BI

SELECT TOP 10 * FROM FactPatientVisits;

--DATA TRANSFORMATION FOR POWERBI

CREATE VIEW vw_HealthcareAnalytics AS
SELECT 
    f.VisitID,
    d.DateID,

    -- Convert Month column to DATE first, then extract month number and name
    MONTH(CAST(d.Month AS DATE)) AS VisitMonthNumber,
    DATENAME(MONTH, CAST(d.Month AS DATE)) AS VisitMonthName,

    -- Extract correct year from Month column
    YEAR(CAST(d.Month AS DATE)) AS VisitYear,

    -- Ensure Day is extracted properly
    DAY(CAST(d.Day AS DATE)) AS VisitDay,

    h.HospitalID,
    h.HospitalName,
    h.Location AS HospitalLocation,
    f.InsuranceID,  
    p.PatientID,

    -- Remove numbers from PatientName but keep full name
    TRIM(TRANSLATE(p.PatientName, '0123456789', '          ')) AS PatientName,

    p.Gender AS PatientGender,
    p.Age AS PatientAge,
    ph.PhysicianID,

    -- Remove numbers from PhysicianName but keep full name
    TRIM(TRANSLATE(ph.PhysicianName, '0123456789', '          ')) AS PhysicianName,

    ph.Specialty AS PhysicianSpecialty,
    f.Diagnosis,
    f.TotalCost,
    
    -- Quarter and Season
    CASE 
        WHEN MONTH(CAST(d.Month AS DATE)) IN (1, 2, 3) THEN 'Q1'
        WHEN MONTH(CAST(d.Month AS DATE)) IN (4, 5, 6) THEN 'Q2'
        WHEN MONTH(CAST(d.Month AS DATE)) IN (7, 8, 9) THEN 'Q3'
        WHEN MONTH(CAST(d.Month AS DATE)) IN (10, 11, 12) THEN 'Q4'
    END AS Quarter,
    
    CASE 
        WHEN MONTH(CAST(d.Month AS DATE)) BETWEEN 1 AND 3 THEN 'Winter'
        WHEN MONTH(CAST(d.Month AS DATE)) BETWEEN 4 AND 6 THEN 'Spring'
        WHEN MONTH(CAST(d.Month AS DATE)) BETWEEN 7 AND 9 THEN 'Summer'
        WHEN MONTH(CAST(d.Month AS DATE)) BETWEEN 10 AND 12 THEN 'Fall'
    END AS Season,

    CASE 
        WHEN p.Age < 18 THEN 'Child'
        WHEN p.Age BETWEEN 18 AND 65 THEN 'Adult'
        ELSE 'Senior'
    END AS AgeGroup,

    DATEDIFF(WEEK, '1900-01-01', CAST(d.Day AS DATE)) AS WeekNumber,
    DATENAME(WEEKDAY, CAST(d.Day AS DATE)) AS DayOfWeek
FROM FactPatientVisits f
LEFT JOIN DimDate d ON f.DateID = d.DateID
LEFT JOIN DimHospitals h ON f.HospitalID = h.HospitalID
LEFT JOIN DimPatients p ON f.PatientID = p.PatientID
LEFT JOIN DimPhysicians ph ON f.PhysicianID = ph.PhysicianID;


SELECT * FROM vw_HealthcareAnalytics;

CREATE VIEW vw_DimInsurance AS
SELECT 
    InsuranceID,
    ProviderName AS InsuranceProvider,
    PlanType AS InsurancePlan
FROM DimInsurance
WHERE ProviderName IS NOT NULL AND PlanType IS NOT NULL; -- Remove unknown providers and plans


SELECT * FROM vw_DimInsurance;

CREATE VIEW vw_HealthcareFinal AS
SELECT * FROM vw_HealthcareAnalytics
LEFT JOIN
SELECT * FROM vw_DimInsurance
;
SELECT * FROM DimDate;

SELECT * FROM vw_HealthcareFinal;