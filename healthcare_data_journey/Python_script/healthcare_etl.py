# extraction of data from azure blob storage and load into DimHospitals
import pandas as pd
from sqlalchemy import create_engine
from io import StringIO
from azure.storage.blob import BlobServiceClient

# Azure Blob Storage Configuration
AZURE_CONNECTION_STRING = "******"
CONTAINER_NAME = "oriowo-container"
BLOB_NAME = "organizations.csv"

# SQL Server Configuration
SQL_SERVER = r"ORIOWO\SQLEXPRESS01"
DATABASE = "HealthcareDB"
TABLE_NAME = "DimHospitals"

# Create Azure Blob Storage Client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)

# Download CSV file from Blob Storage
blob_data = blob_client.download_blob().content_as_text()

# Load into Pandas DataFrame using StringIO
df = pd.read_csv(StringIO(blob_data))

# Select and rename columns to match SQL Server table
df_selected = df[['Id', 'NAME', 'ADDRESS', 'CITY', 'STATE']].copy()  # Make a copy to avoid warning
df_selected.columns = ['HospitalID', 'HospitalName', 'Address', 'City', 'State']

print(df_selected.head())  # Show the first few rows of the DataFrame

# Combine Address, City, and State into one "Location" column
df_selected['Location'] = df_selected[['Address', 'City', 'State']].astype(str).agg(', '.join, axis=1)

# Drop the separate columns after merging
df_selected = df_selected[['HospitalID', 'HospitalName', 'Location']]

# Convert HospitalID to string (UUID)
df_selected['HospitalID'] = df_selected['HospitalID'].astype(str)

# Create SQLAlchemy connection to SQL Server
engine = create_engine(f"mssql+pyodbc://@{SQL_SERVER}/{DATABASE}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes")

# Insert data into SQL Server
try:
    # Insert data into the SQL Server table (ensure HospitalID is treated as a string)
    df_selected.to_sql(TABLE_NAME, engine, if_exists='append', index=False)
    print("Data successfully inserted into SQL Server.")
except Exception as e:
    print(f"Error during data insertion: {e}")

#testing of extraction and loading into sql
import sqlalchemy
from sqlalchemy import text

# SQL Server Configuration
SQL_SERVER = r"ORIOWO\SQLEXPRESS01"
DATABASE = "HealthcareDB"

# Create SQLAlchemy connection to SQL Server
engine = sqlalchemy.create_engine(f"mssql+pyodbc://@{SQL_SERVER}/{DATABASE}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes")

# Query to count rows in DimHospitals using SQLAlchemy's text() function
with engine.connect() as connection:
    result = connection.execute(text("SELECT COUNT(*) FROM DimHospitals"))
    row_count = result.scalar()  # scalar() returns the first column of the first row

print(f"Number of rows in DimHospitals: {row_count}")

# extraction of data from azure blob storage and load into DimPhysicians
import pandas as pd
from sqlalchemy import create_engine
from io import StringIO
from azure.storage.blob import BlobServiceClient

# Azure Blob Storage Configuration
AZURE_CONNECTION_STRING = "******"
CONTAINER_NAME = "oriowo-container"
BLOB_NAME = "providers.csv"

# SQL Server Configuration
SQL_SERVER = r"ORIOWO\SQLEXPRESS01"
DATABASE = "HealthcareDB"
TABLE_NAME = "DimPhysicians"

# Create Azure Blob Storage Client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)

# Download CSV file from Blob Storage
blob_data = blob_client.download_blob().content_as_text()

# Load into Pandas DataFrame using StringIO
df = pd.read_csv(StringIO(blob_data))

# Select and rename columns to match SQL Server table
df_selected = df[['Id', 'NAME', 'SPECIALITY']].copy()  # Make a copy to avoid warning
df_selected.columns = ['PhysicianID', 'PhysicianName', 'Specialty']

print(df_selected.head())  # Show the first few rows of the DataFrame

# Drop the separate columns after merging
df_selected = df_selected[['PhysicianID', 'PhysicianName', 'Specialty']]

# Convert PhysicianID to string (UUID)
df_selected['PhysicianID'] = df_selected['PhysicianID'].astype(str)

# Create SQLAlchemy connection to SQL Server
engine = create_engine(f"mssql+pyodbc://@{SQL_SERVER}/{DATABASE}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes")

# Insert data into SQL Server
try:
    # Insert data into the SQL Server table (ensure HospitalID is treated as a string)
    df_selected.to_sql(TABLE_NAME, engine, if_exists='append', index=False)
    print("Data successfully inserted into SQL Server.")
except Exception as e:
    print(f"Error during data insertion: {e}")

# extraction of data from azure blob storage and load into DimPatients
import pandas as pd
from sqlalchemy import create_engine
from io import StringIO
from azure.storage.blob import BlobServiceClient
from datetime import datetime

# Azure Blob Storage Configuration
AZURE_CONNECTION_STRING = "******"
CONTAINER_NAME = "oriowo-container"
BLOB_NAME = "patients.csv"

# SQL Server Configuration
SQL_SERVER = r"ORIOWO\SQLEXPRESS01"
DATABASE = "HealthcareDB"
TABLE_NAME = "DimPatients"

# Create Azure Blob Storage Client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)

# Download CSV file from Blob Storage
blob_data = blob_client.download_blob().content_as_text()

# Load into Pandas DataFrame
df = pd.read_csv(StringIO(blob_data))

# Select necessary columns
df_selected = df[['Id', 'GENDER', 'BIRTHDATE', 'PREFIX', 'FIRST', 'LAST', 'ADDRESS', 'COUNTY', 'CITY', 'STATE']].copy()

# Rename columns to match SQL Server
df_selected.columns = ['PatientID', 'Gender', 'BirthDate', 'Prefix', 'FirstName', 'LastName', 'Address', 'County', 'City', 'State']

# Convert BirthDate (YYYY-MM-DD) to Age (INT)
df_selected['BirthDate'] = pd.to_datetime(df_selected['BirthDate'], errors='coerce')  # Convert to datetime
df_selected['Age'] = df_selected['BirthDate'].apply(lambda x: datetime.now().year - x.year if pd.notnull(x) else None)

# Combine Prefix, FirstName, and LastName into "PatientName"
df_selected['PatientName'] = df_selected[['Prefix', 'FirstName', 'LastName']].astype(str).agg(' '.join, axis=1)

# Combine Address, County, City, and State into "Address"
df_selected['Address'] = df_selected[['Address', 'County', 'City', 'State']].astype(str).agg(', '.join, axis=1)

# Keep only required columns
df_selected = df_selected[['PatientID', 'PatientName', 'Age', 'Gender', 'Address']]

# Convert PatientID to string
df_selected['PatientID'] = df_selected['PatientID'].astype(str)

# Print first few rows to verify transformation
print(df_selected.head())

# Create SQLAlchemy connection to SQL Server
engine = create_engine(f"mssql+pyodbc://@{SQL_SERVER}/{DATABASE}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes")

# Insert data into SQL Server
try:
    df_selected.to_sql(TABLE_NAME, engine, if_exists='append', index=False)
    print("Data successfully inserted into SQL Server.")
except Exception as e:
    print(f"Error during data insertion: {e}")
    
# extraction of data from azure blob storage and load into DimInsurance    
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from io import StringIO
from azure.storage.blob import BlobServiceClient

# Azure Blob Storage Configuration
AZURE_CONNECTION_STRING = "*******"
CONTAINER_NAME = "oriowo-container"

# File names in Azure Blob Storage
CAREPLAN_BLOB = "careplans.csv"
HEALTHCARE_DATASET_BLOB = "healthcare_dataset.csv"

# SQL Server Configuration
SQL_SERVER = r"ORIOWO\SQLEXPRESS01"
DATABASE = "HealthcareDB"
TABLE_NAME = "DimInsurance"

# Create Azure Blob Storage Client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)

def load_csv_from_blob(blob_name):
    """Helper function to load CSV from Azure Blob Storage"""
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
    blob_data = blob_client.download_blob().readall().decode('utf-8')  # Force fresh download
    return pd.read_csv(StringIO(blob_data))

# Load data from Azure Blob Storage
df_careplans = load_csv_from_blob(CAREPLAN_BLOB)
df_healthcare_dataset = load_csv_from_blob(HEALTHCARE_DATASET_BLOB)

# Select relevant columns
df_careplans_selected = df_careplans[['PATIENT', 'DESCRIPTION']].copy()
df_healthcare_dataset_selected = df_healthcare_dataset[['Insurance Provider']].copy()

# Rename columns for consistency
df_careplans_selected.columns = ['InsuranceID', 'PlanType']
df_healthcare_dataset_selected.columns = ['ProviderName']

# Merge to create the final insurance table
df_insurance = df_careplans_selected.merge(df_healthcare_dataset_selected, left_index=True, right_index=True, how="left")

# Keep only required columns
df_insurance = df_insurance[['InsuranceID', 'PlanType', 'ProviderName']]

# Drop duplicates to ensure unique InsuranceID-PlanType combinations
df_insurance = df_insurance.drop_duplicates(subset=['InsuranceID', 'PlanType'])

# Get unique provider names (excluding NULL & "No Insurance")
available_providers = df_healthcare_dataset_selected['ProviderName'].dropna().unique()
available_providers = [provider for provider in available_providers if provider.lower() != 'no insurance']

# Replace NULL and "No Insurance" with random valid providers
if len(available_providers) > 0:
    df_insurance.loc[df_insurance['ProviderName'].isna() | (df_insurance['ProviderName'].str.lower() == 'no insurance'), 'ProviderName'] = np.random.choice(available_providers, size=df_insurance['ProviderName'].isna().sum(), replace=True)

# Convert InsuranceID to string
df_insurance['InsuranceID'] = df_insurance['InsuranceID'].astype(str)

# Print sample data before inserting
print(df_insurance.sample(10))

# Ensure there are no NULLs in ProviderName
assert df_insurance['ProviderName'].isna().sum() == 0, "There are still NULL values in ProviderName!"

# Create SQLAlchemy connection to SQL Server
engine = create_engine(f"mssql+pyodbc://@{SQL_SERVER}/{DATABASE}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes")

# Insert data into SQL Server (replace old data)
try:
    df_insurance.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
    print("Data successfully inserted into SQL Server.")
except Exception as e:
    print(f"Error during data insertion: {e}")

# extraction of data from azure blob storage and load into DimDate
#generation of random uuid for dateid
import pandas as pd
from sqlalchemy import create_engine
from io import StringIO
from azure.storage.blob import BlobServiceClient
import uuid

# Azure Blob Storage Configuration
AZURE_CONNECTION_STRING = "******"
CONTAINER_NAME = "oriowo-container"
BLOB_NAME = "immunizations.csv"

# SQL Server Configuration
SQL_SERVER = r"ORIOWO\SQLEXPRESS01"
DATABASE = "HealthcareDB"
TABLE_NAME = "DimDate"

# Create Azure Blob Storage Client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)

# Download CSV file from Blob Storage
blob_data = blob_client.download_blob().content_as_text()

# Load into Pandas DataFrame using StringIO
df = pd.read_csv(StringIO(blob_data))

# Check the structure of the DataFrame
print(df.head())  # Check the first few rows to understand the structure

# Convert DATE column to datetime format (ensure no time component)
df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')

# Drop rows with invalid/missing dates
df = df.dropna(subset=['DATE'])

# Extract unique dates from the DATA column
df_dates = df[['DATE']].drop_duplicates().reset_index(drop=True)

# Generate unique DateID using UUID
df_dates['DateID'] = [str(uuid.uuid4()) for _ in range(len(df_dates))]

# Extract Year, Month, and Day
df_dates['Year'] = df_dates['DATE'].dt.year
df_dates['Month'] = df_dates['DATE'].dt.strftime('%Y-%m-%d')  # Keep as datetime format
df_dates['Day'] = df_dates['DATE'].dt.strftime('%Y-%m-%d')    # Keep as datetime format

# Rename columns to match DimDate table structure
df_dates = df_dates[['DateID', 'Year', 'Month', 'Day']]

# Check the resulting DataFrame
print(df_dates.head())

# Create SQLAlchemy connection to SQL Server
engine = create_engine(f"mssql+pyodbc://@{SQL_SERVER}/{DATABASE}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes")

# Insert data into SQL Server (append to existing table)
try:
    # Insert data into the DimDate table
    df_dates.to_sql(TABLE_NAME, engine, if_exists='append', index=False)
    print("Data successfully inserted into DimDate table.")
except Exception as e:
    print(f"Error during data insertion: {e}")


# extraction of data from azure blob storage and load into FactPatientVisits
# using the data created in each primary Key
import pandas as pd
import uuid
from sqlalchemy import create_engine, text
from io import StringIO
from azure.storage.blob import BlobServiceClient

# Azure Blob Storage Configuration
AZURE_CONNECTION_STRING = "*******"
CONTAINER_NAME = "oriowo-container"

# SQL Server Configuration
SQL_SERVER = r"ORIOWO\SQLEXPRESS01"
DATABASE = "HealthcareDB"
TABLE_NAME = "FactPatientVisits"  

# Create SQLAlchemy connection to SQL Server
engine = create_engine(f"mssql+pyodbc://@{SQL_SERVER}/{DATABASE}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes")

# Create Azure Blob Storage Client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)

def load_csv_from_blob(blob_name):
    """Download CSV file from Azure Blob Storage and return it as a Pandas DataFrame."""
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
    blob_data = blob_client.download_blob().content_as_text()
    return pd.read_csv(StringIO(blob_data))

def fetch_valid_ids(table_name, column_name):
    """Fetch valid IDs from a specific column in a Dim table."""
    query = f"SELECT {column_name} FROM {table_name}"
    with engine.connect() as connection:
        result = connection.execute(text(query)).fetchall()  # Fetch all rows
        return [row[0] for row in result]  # Access column values in each tuple

# Load data from CSV files
df_careplans = load_csv_from_blob("careplans.csv")
df_organizations = load_csv_from_blob("organizations.csv")
df_providers = load_csv_from_blob("providers.csv")
df_patients = load_csv_from_blob("patients.csv")
df_healthcare = load_csv_from_blob("healthcare_dataset.csv")

# Fetch valid IDs from Dim tables
valid_hospital_ids = fetch_valid_ids("DimHospitals", "HospitalID")
valid_physician_ids = fetch_valid_ids("DimPhysicians", "PhysicianID")
valid_insurance_ids = fetch_valid_ids("DimInsurance", "InsuranceID")
valid_patient_ids = fetch_valid_ids("DimPatients", "PatientID")

# Extract required columns and generate UUID for VisitID
df_fact = pd.DataFrame({
    'VisitID': [str(uuid.uuid4()) for _ in range(len(df_careplans))],  # Generating UUID for VisitID
    'HospitalID': df_organizations['Id'],
    'PhysicianID': df_providers['Id'],
    'InsuranceID': df_careplans['Id'],
    'PatientID': df_patients['Id'],
    'TotalCost': df_patients['HEALTHCARE_EXPENSES'],
    'Diagnosis': df_healthcare['Medical Condition'],
    'DateID': 'Placeholder'  # Placeholder for DateID
})

# Filter the data to only include valid IDs
df_fact = df_fact[
    df_fact['HospitalID'].isin(valid_hospital_ids) &
    df_fact['PhysicianID'].isin(valid_physician_ids) &
    df_fact['InsuranceID'].isin(valid_insurance_ids) &
    df_fact['PatientID'].isin(valid_patient_ids)
]

# Fetch valid DateIDs from the DimDate table
valid_date_ids = fetch_valid_ids("DimDate", "DateID")

# Replace 'Placeholder' with valid DateID values
df_fact['DateID'] = df_fact['DateID'].apply(lambda x: valid_date_ids[0] if x == 'Placeholder' else x)

# Convert columns to appropriate data types
df_fact = df_fact.astype(str)  # Ensure all columns are strings to prevent type mismatch

# Insert data into SQL Server
try:
    df_fact.to_sql(TABLE_NAME, engine, if_exists='append', index=False)
    print("Data successfully inserted into FactPatientVisits table.")
except Exception as e:
    print(f"Error during data insertion: {e}")