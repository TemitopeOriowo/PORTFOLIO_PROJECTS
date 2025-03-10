CREATE DATABASE HealthcareDB;
GO

CREATE TABLE DimDate
( 
    DateID               varchar(100)  NOT NULL ,
    DateValue            datetime  NOT NULL ,
    Year                 integer  NOT NULL ,
    Month                datetime  NOT NULL ,
    Day                  datetime  NOT NULL 
);
GO

ALTER TABLE DimDate
    ADD CONSTRAINT PK_DimDate_DateID PRIMARY KEY (DateID);
GO
--THIS COLUMN WAS DROPPED AS IT WAS NOT NEEDED FOR BUSINESS INSIGHT

ALTER TABLE DimDate
DROP COLUMN DateValue;

CREATE TABLE DimHospitals
( 
    HospitalID           varchar(225)  NOT NULL , 
    HospitalName         varchar(100)  NOT NULL ,
    Location             varchar(100) NOT NULL 
);
GO

ALTER TABLE DimHospitals
    ADD CONSTRAINT PK_DimHospitals_HospID PRIMARY KEY (HospitalID);  
GO

CREATE TABLE DimInsurance
( 
    InsuranceID          varchar(225)  NOT NULL ,
    ProviderName         varchar(100)  NOT NULL ,
    PlanType             varchar(100)  NOT NULL 
);
GO

ALTER TABLE DimInsurance
    ADD CONSTRAINT PK_DimInsurance_InsID PRIMARY KEY (InsuranceID);
GO

ALTER TABLE DimInsurance
	DROP CONSTRAINT PK_DimInsurance_InsID;

ALTER TABLE DimInsurance
 ALTER COLUMN InsuranceID  varchar(225)  NULL;
   
   
ALTER TABLE DimInsurance
 ALTER COLUMN ProviderName varchar(100)  NULL;

ALTER TABLE DimInsurance
 ALTER COLUMN PlanType varchar(100) NULL;

CREATE TABLE DimPatients
( 
    PatientID            varchar(225)  NOT NULL ,
    PatientName          varchar(100)  NOT NULL ,
    Gender               varchar(10)  NOT NULL ,
    Age                  integer  NOT NULL ,
    Address              varchar(225)  NOT NULL 
);
GO

ALTER TABLE DimPatients
    ADD CONSTRAINT PK_DimPatients_PatID PRIMARY KEY (PatientID);
GO

CREATE TABLE DimPhysicians
( 
    PhysicianID          varchar(225)  NOT NULL ,
    PhysicianName        varchar(100)  NOT NULL ,
    Specialty            varchar(100)  NOT NULL 
);
GO

ALTER TABLE DimPhysicians
ALTER COLUMN PhysicianID varchar(225) NOT NULL;

ALTER TABLE DimPhysicians
    ADD CONSTRAINT PK_DimPhysicians_PhyID PRIMARY KEY (PhysicianID);
GO

--ALL FOREIGN KEYS AND PRIMARY KEY VISITID  DATATYPE WAS CHANGED
--BEFORE CREATING THE TABLE IN SQL SERVER
CREATE TABLE FactPatientVisits
( 
    VisitID              varchar(225)  NOT NULL ,
    HospitalID           varchar(225)  NOT NULL ,  
    PhysicianID          varchar(225)  NOT NULL ,
    InsuranceID          varchar(225)  NOT NULL ,
    DateID               varchar(100)  NOT NULL ,
    PatientID            varchar(225)  NOT NULL ,
    Diagnosis            varchar(70)  NOT NULL ,
    TotalCost            decimal(10,2)  NOT NULL 
);
GO


ALTER TABLE FactPatientVisits
    ADD CONSTRAINT PK_FPatientVisits_VisitID PRIMARY KEY (VisitID);
GO

ALTER TABLE FactPatientVisits
    ADD CONSTRAINT FK_DimPatients_PatientID FOREIGN KEY (PatientID) REFERENCES DimPatients(PatientID);
GO

ALTER TABLE FactPatientVisits
    ADD CONSTRAINT DimHospitalsID_HostID FOREIGN KEY (HospitalID) REFERENCES DimHospitals(HospitalID);
GO

ALTER TABLE FactPatientVisits
    ADD CONSTRAINT FK_DimPhy_PhyID FOREIGN KEY (PhysicianID) REFERENCES DimPhysicians(PhysicianID);
GO

ALTER TABLE FactPatientVisits
    ADD CONSTRAINT FK_DimInsID_InsID FOREIGN KEY (InsuranceID) REFERENCES DimInsurance(InsuranceID);
GO
ALTER TABLE FactPatientVisits
	DROP CONSTRAINT FK_DimInsID_InsID;

ALTER TABLE FactPatientVisits
    ADD CONSTRAINT FK_DimDate_DateID FOREIGN KEY (DateID) REFERENCES DimDate(DateID);
GO

--SCRIPTS TO TEST DATA INGESTION

SELECT *from DimHospitals;

SELECT TOP 10 * FROM DimHospitals;

SELECT  * FROM DimPhysicians;

SELECT  TOP 10 * FROM DimPatients;

SELECT  * FROM DimInsurance;

SELECT * FROM DimDate;

SELECT * FROM FactPatientVisits;


