-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create Schemas
CREATE SCHEMA IF NOT EXISTS Person;
CREATE SCHEMA IF NOT EXISTS HumanResources;
CREATE SCHEMA IF NOT EXISTS Production;
CREATE SCHEMA IF NOT EXISTS Purchasing;
CREATE SCHEMA IF NOT EXISTS Sales;
CREATE SCHEMA IF NOT EXISTS dbo; -- Standard SQL Server default schema

-------------------------------------------------------------------------------
-- dbo Schema
-------------------------------------------------------------------------------

CREATE TABLE dbo.AWBuildVersion (
    SystemInformationID SERIAL PRIMARY KEY, -- Assuming smallint in original, SERIAL is fine
    Database_Version VARCHAR(25) NOT NULL,
    VersionDate TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dbo.DatabaseLog (
    DatabaseLogID SERIAL PRIMARY KEY,
    PostTime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    DatabaseUser VARCHAR(128) NOT NULL,
    Event VARCHAR(128) NOT NULL,
    SchemaArg VARCHAR(128), -- Renamed from Schema to avoid keyword conflict
    ObjectArg VARCHAR(128), -- Renamed from Object to avoid keyword conflict
    TSQL TEXT NOT NULL,
    XmlEvent XML NOT NULL
);

CREATE TABLE dbo.ErrorLog (
    ErrorLogID SERIAL PRIMARY KEY,
    ErrorTime TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UserName VARCHAR(128) NOT NULL, -- sysname equivalent
    ErrorNumber INTEGER NOT NULL,
    ErrorSeverity INTEGER,
    ErrorState INTEGER,
    ErrorProcedure VARCHAR(126),
    ErrorLine INTEGER,
    ErrorMessage VARCHAR(4000) NOT NULL
);


-------------------------------------------------------------------------------
-- Person Schema
-------------------------------------------------------------------------------
-- Tables with no or few external FK dependencies first

CREATE TABLE Person.CountryRegion (
    CountryRegionCode VARCHAR(3) PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_CountryRegion_Name UNIQUE (Name)
);

CREATE TABLE Person.AddressType (
    AddressTypeID SERIAL PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_AddressType_Name UNIQUE (Name)
);

CREATE TABLE Person.ContactType (
    ContactTypeID SERIAL PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_ContactType_Name UNIQUE (Name)
);

CREATE TABLE Person.PhoneNumberType (
    PhoneNumberTypeID SERIAL PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_PhoneNumberType_Name UNIQUE (Name)
);

-- BusinessEntity is a parent for Person, Store, Vendor
CREATE TABLE Person.BusinessEntity (
    BusinessEntityID SERIAL PRIMARY KEY,
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Person.Person (
    BusinessEntityID INTEGER PRIMARY KEY REFERENCES Person.BusinessEntity(BusinessEntityID),
    PersonType CHAR(2) NOT NULL,
    NameStyle BOOLEAN NOT NULL DEFAULT FALSE,
    Title VARCHAR(8),
    FirstName VARCHAR(50) NOT NULL,
    MiddleName VARCHAR(50),
    LastName VARCHAR(50) NOT NULL,
    Suffix VARCHAR(10),
    EmailPromotion INTEGER NOT NULL DEFAULT 0,
    AdditionalContactInfo XML,
    Demographics XML,
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
    -- CHECK (PersonType IN ('SC', 'VC', 'GC', 'IN', 'EM', 'SP')) -- Example Check
);

CREATE TABLE Person.Password (
    BusinessEntityID INTEGER PRIMARY KEY REFERENCES Person.Person(BusinessEntityID),
    PasswordHash VARCHAR(128) NOT NULL,
    PasswordSalt VARCHAR(10) NOT NULL,
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Person.EmailAddress (
    BusinessEntityID INTEGER NOT NULL REFERENCES Person.Person(BusinessEntityID),
    EmailAddressID SERIAL NOT NULL,
    EmailAddress VARCHAR(50),
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (BusinessEntityID, EmailAddressID)
);

CREATE TABLE Person.PersonPhone (
    BusinessEntityID INTEGER NOT NULL REFERENCES Person.Person(BusinessEntityID),
    PhoneNumber VARCHAR(25) NOT NULL, -- Phone type in MS
    PhoneNumberTypeID INTEGER NOT NULL REFERENCES Person.PhoneNumberType(PhoneNumberTypeID),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (BusinessEntityID, PhoneNumber, PhoneNumberTypeID)
);

-- StateProvince depends on CountryRegion and (later) SalesTerritory
CREATE TABLE Person.StateProvince (
    StateProvinceID SERIAL PRIMARY KEY,
    StateProvinceCode CHAR(3) NOT NULL,
    CountryRegionCode VARCHAR(3) NOT NULL REFERENCES Person.CountryRegion(CountryRegionCode),
    IsOnlyStateProvinceFlag BOOLEAN NOT NULL DEFAULT TRUE,
    Name VARCHAR(50) NOT NULL,
    TerritoryID INTEGER, -- FK to Sales.SalesTerritory added later if needed for ordering
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_StateProvince_Name UNIQUE (Name),
    CONSTRAINT UQ_StateProvince_StateProvinceCode_CountryRegionCode UNIQUE (StateProvinceCode, CountryRegionCode),
    CONSTRAINT UQ_StateProvince_rowguid UNIQUE (rowguid)
);

CREATE TABLE Person.Address (
    AddressID SERIAL PRIMARY KEY,
    AddressLine1 VARCHAR(60) NOT NULL,
    AddressLine2 VARCHAR(60),
    City VARCHAR(30) NOT NULL,
    StateProvinceID INTEGER NOT NULL REFERENCES Person.StateProvince(StateProvinceID),
    PostalCode VARCHAR(15) NOT NULL,
    SpatialLocation TEXT, -- Placeholder for GEOGRAPHY
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_Address_rowguid UNIQUE (rowguid)
    -- CONSTRAINT UQ_Address_AddressLine1_AddressLine2_City_StateProvinceID_PostalCode UNIQUE (AddressLine1, AddressLine2, City, StateProvinceID, PostalCode) -- From U2 on diagram
);

CREATE TABLE Person.BusinessEntityAddress (
    BusinessEntityID INTEGER NOT NULL REFERENCES Person.BusinessEntity(BusinessEntityID),
    AddressID INTEGER NOT NULL REFERENCES Person.Address(AddressID),
    AddressTypeID INTEGER NOT NULL REFERENCES Person.AddressType(AddressTypeID),
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (BusinessEntityID, AddressID, AddressTypeID),
    CONSTRAINT UQ_BusinessEntityAddress_rowguid UNIQUE (rowguid)
);

CREATE TABLE Person.BusinessEntityContact (
    BusinessEntityID INTEGER NOT NULL REFERENCES Person.BusinessEntity(BusinessEntityID),
    PersonID INTEGER NOT NULL REFERENCES Person.Person(BusinessEntityID), -- Contact Person
    ContactTypeID INTEGER NOT NULL REFERENCES Person.ContactType(ContactTypeID),
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (BusinessEntityID, PersonID, ContactTypeID),
    CONSTRAINT UQ_BusinessEntityContact_rowguid UNIQUE (rowguid)
);


-------------------------------------------------------------------------------
-- HumanResources Schema
-------------------------------------------------------------------------------

CREATE TABLE HumanResources.Department (
    DepartmentID SERIAL PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    GroupName VARCHAR(50) NOT NULL,
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_Department_Name UNIQUE (Name)
);

CREATE TABLE HumanResources.Shift (
    ShiftID SERIAL PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    StartTime TIME WITHOUT TIME ZONE NOT NULL,
    EndTime TIME WITHOUT TIME ZONE NOT NULL,
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_Shift_Name UNIQUE (Name),
    CONSTRAINT UQ_Shift_StartTime_EndTime UNIQUE (StartTime, EndTime)
);

CREATE TABLE HumanResources.Employee (
    BusinessEntityID INTEGER PRIMARY KEY REFERENCES Person.Person(BusinessEntityID),
    NationalIDNumber VARCHAR(15) NOT NULL,
    LoginID VARCHAR(256) NOT NULL,
    -- OrganizationNode VARCHAR(255), -- HIERARCHYID placeholder
    -- OrganizationLevel SMALLINT, -- HIERARCHYID derived
    JobTitle VARCHAR(50) NOT NULL,
    BirthDate DATE NOT NULL,
    MaritalStatus CHAR(1) NOT NULL,
    Gender CHAR(1) NOT NULL,
    HireDate DATE NOT NULL,
    SalariedFlag BOOLEAN NOT NULL DEFAULT TRUE,
    VacationHours SMALLINT NOT NULL DEFAULT 0,
    SickLeaveHours SMALLINT NOT NULL DEFAULT 0,
    CurrentFlag BOOLEAN NOT NULL DEFAULT TRUE,
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_Employee_LoginID UNIQUE (LoginID),
    CONSTRAINT UQ_Employee_NationalIDNumber UNIQUE (NationalIDNumber),
    CONSTRAINT UQ_Employee_rowguid UNIQUE (rowguid)
    -- CHECK (Gender IN ('M', 'F')),
    -- CHECK (MaritalStatus IN ('M', 'S'))
);

CREATE TABLE HumanResources.EmployeeDepartmentHistory (
    BusinessEntityID INTEGER NOT NULL REFERENCES HumanResources.Employee(BusinessEntityID),
    DepartmentID INTEGER NOT NULL REFERENCES HumanResources.Department(DepartmentID),
    ShiftID INTEGER NOT NULL REFERENCES HumanResources.Shift(ShiftID),
    StartDate DATE NOT NULL,
    EndDate DATE,
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (BusinessEntityID, StartDate, DepartmentID, ShiftID)
);

CREATE TABLE HumanResources.EmployeePayHistory (
    BusinessEntityID INTEGER NOT NULL REFERENCES HumanResources.Employee(BusinessEntityID),
    RateChangeDate TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    Rate NUMERIC(19,4) NOT NULL, -- money
    PayFrequency SMALLINT NOT NULL,
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (BusinessEntityID, RateChangeDate)
);

CREATE TABLE HumanResources.JobCandidate (
    JobCandidateID SERIAL PRIMARY KEY,
    BusinessEntityID INTEGER REFERENCES HumanResources.Employee(BusinessEntityID), -- Can be null if not an existing employee
    Resume XML,
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-------------------------------------------------------------------------------
-- Production Schema
-------------------------------------------------------------------------------

CREATE TABLE Production.UnitMeasure (
    UnitMeasureCode CHAR(3) PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_UnitMeasure_Name UNIQUE (Name)
);

CREATE TABLE Production.ProductCategory (
    ProductCategoryID SERIAL PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_ProductCategory_Name UNIQUE (Name),
    CONSTRAINT UQ_ProductCategory_rowguid UNIQUE (rowguid)
);

CREATE TABLE Production.ProductSubcategory (
    ProductSubcategoryID SERIAL PRIMARY KEY,
    ProductCategoryID INTEGER NOT NULL REFERENCES Production.ProductCategory(ProductCategoryID),
    Name VARCHAR(50) NOT NULL,
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_ProductSubcategory_Name UNIQUE (Name),
    CONSTRAINT UQ_ProductSubcategory_rowguid UNIQUE (rowguid)
);

CREATE TABLE Production.ProductModel (
    ProductModelID SERIAL PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    CatalogDescription XML,
    Instructions XML,
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_ProductModel_Name UNIQUE (Name),
    CONSTRAINT UQ_ProductModel_rowguid UNIQUE (rowguid)
);

CREATE TABLE Production.Product (
    ProductID SERIAL PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    ProductNumber VARCHAR(25) NOT NULL,
    MakeFlag BOOLEAN NOT NULL DEFAULT TRUE,
    FinishedGoodsFlag BOOLEAN NOT NULL DEFAULT TRUE,
    Color VARCHAR(15),
    SafetyStockLevel SMALLINT NOT NULL,
    ReorderPoint SMALLINT NOT NULL,
    StandardCost NUMERIC(19,4) NOT NULL, -- money
    ListPrice NUMERIC(19,4) NOT NULL, -- money
    Size VARCHAR(5),
    SizeUnitMeasureCode CHAR(3) REFERENCES Production.UnitMeasure(UnitMeasureCode),
    WeightUnitMeasureCode CHAR(3) REFERENCES Production.UnitMeasure(UnitMeasureCode),
    Weight NUMERIC(8,2),
    DaysToManufacture INTEGER NOT NULL,
    ProductLine CHAR(2),
    Class CHAR(2),
    Style CHAR(2),
    ProductSubcategoryID INTEGER REFERENCES Production.ProductSubcategory(ProductSubcategoryID),
    ProductModelID INTEGER REFERENCES Production.ProductModel(ProductModelID),
    SellStartDate TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    SellEndDate TIMESTAMP WITHOUT TIME ZONE,
    DiscontinuedDate TIMESTAMP WITHOUT TIME ZONE,
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_Product_Name UNIQUE (Name),
    CONSTRAINT UQ_Product_ProductNumber UNIQUE (ProductNumber),
    CONSTRAINT UQ_Product_rowguid UNIQUE (rowguid)
    -- CHECK (ProductLine IN ('R','M','T','S')),
    -- CHECK (Class IN ('H','M','L')),
    -- CHECK (Style IN ('W','M','U'))
);

CREATE TABLE Production.BillOfMaterials (
    BillOfMaterialsID SERIAL PRIMARY KEY,
    ProductAssemblyID INTEGER REFERENCES Production.Product(ProductID),
    ComponentID INTEGER NOT NULL REFERENCES Production.Product(ProductID),
    StartDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    EndDate TIMESTAMP WITHOUT TIME ZONE,
    UnitMeasureCode CHAR(3) NOT NULL REFERENCES Production.UnitMeasure(UnitMeasureCode),
    BOMLevel SMALLINT NOT NULL,
    PerAssemblyQty NUMERIC(8,2) NOT NULL DEFAULT 1.00,
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_BillOfMaterials_ProductAssemblyID_ComponentID_StartDate UNIQUE (ProductAssemblyID, ComponentID, StartDate)
);

CREATE TABLE Production.ProductCostHistory (
    ProductID INTEGER NOT NULL REFERENCES Production.Product(ProductID),
    StartDate TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    EndDate TIMESTAMP WITHOUT TIME ZONE,
    StandardCost NUMERIC(19,4) NOT NULL, -- money
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ProductID, StartDate)
);

CREATE TABLE Production.ProductListPriceHistory (
    ProductID INTEGER NOT NULL REFERENCES Production.Product(ProductID),
    StartDate TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    EndDate TIMESTAMP WITHOUT TIME ZONE,
    ListPrice NUMERIC(19,4) NOT NULL, -- money
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ProductID, StartDate)
);

CREATE TABLE Production.ProductReview (
    ProductReviewID SERIAL PRIMARY KEY,
    ProductID INTEGER NOT NULL REFERENCES Production.Product(ProductID),
    ReviewerName VARCHAR(50) NOT NULL,
    ReviewDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    EmailAddress VARCHAR(50) NOT NULL,
    Rating INTEGER NOT NULL, -- CHECK (Rating BETWEEN 1 AND 5),
    Comments VARCHAR(3850),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Production.TransactionHistory (
    TransactionID SERIAL PRIMARY KEY,
    ProductID INTEGER NOT NULL REFERENCES Production.Product(ProductID),
    ReferenceOrderID INTEGER NOT NULL,
    ReferenceOrderLineID INTEGER NOT NULL DEFAULT 0,
    TransactionDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    TransactionType CHAR(1) NOT NULL, -- CHECK (TransactionType IN ('W','S','P')),
    Quantity INTEGER NOT NULL,
    ActualCost NUMERIC(19,4) NOT NULL, -- money
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Production.TransactionHistoryArchive (
    TransactionID INTEGER PRIMARY KEY, -- Not SERIAL, archived from TransactionHistory
    ProductID INTEGER NOT NULL, -- No FK, data might be old
    ReferenceOrderID INTEGER NOT NULL,
    ReferenceOrderLineID INTEGER NOT NULL,
    TransactionDate TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    TransactionType CHAR(1) NOT NULL,
    Quantity INTEGER NOT NULL,
    ActualCost NUMERIC(19,4) NOT NULL,
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL
);

CREATE TABLE Production.Location (
    LocationID SERIAL PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    CostRate NUMERIC(19,4) NOT NULL DEFAULT 0.00, -- money
    Availability NUMERIC(8,2) NOT NULL DEFAULT 0.00,
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_Location_Name UNIQUE (Name)
);

CREATE TABLE Production.ProductInventory (
    ProductID INTEGER NOT NULL REFERENCES Production.Product(ProductID),
    LocationID INTEGER NOT NULL REFERENCES Production.Location(LocationID),
    Shelf VARCHAR(10) NOT NULL,
    Bin SMALLINT NOT NULL,
    Quantity SMALLINT NOT NULL DEFAULT 0,
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ProductID, LocationID)
);

CREATE TABLE Production.ScrapReason (
    ScrapReasonID SERIAL PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_ScrapReason_Name UNIQUE (Name)
);

CREATE TABLE Production.WorkOrder (
    WorkOrderID SERIAL PRIMARY KEY,
    ProductID INTEGER NOT NULL REFERENCES Production.Product(ProductID),
    OrderQty INTEGER NOT NULL,
    StockedQty INTEGER NOT NULL, -- GENERATED ALWAYS AS (OrderQty - ScrappedQty) STORED, -- PG12+
    ScrappedQty SMALLINT NOT NULL,
    StartDate TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    EndDate TIMESTAMP WITHOUT TIME ZONE,
    DueDate TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    ScrapReasonID SMALLINT REFERENCES Production.ScrapReason(ScrapReasonID),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Production.WorkOrderRouting (
    WorkOrderID INTEGER NOT NULL REFERENCES Production.WorkOrder(WorkOrderID),
    ProductID INTEGER NOT NULL, -- Specifically the product being made in this step
    OperationSequence SMALLINT NOT NULL,
    LocationID INTEGER NOT NULL REFERENCES Production.Location(LocationID),
    ScheduledStartDate TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    ScheduledEndDate TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    ActualStartDate TIMESTAMP WITHOUT TIME ZONE,
    ActualEndDate TIMESTAMP WITHOUT TIME ZONE,
    ActualResourceHrs NUMERIC(9,4),
    PlannedCost NUMERIC(19,4) NOT NULL, -- money
    ActualCost NUMERIC(19,4), -- money
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (WorkOrderID, ProductID, OperationSequence)
    -- Add FK for ProductID to Production.Product if it is distinct from WorkOrder.ProductID contextually
    -- ALTER TABLE Production.WorkOrderRouting ADD CONSTRAINT FK_WorkOrderRouting_Product FOREIGN KEY (ProductID) REFERENCES Production.Product(ProductID);
);

CREATE TABLE Production.ProductPhoto (
    ProductPhotoID SERIAL PRIMARY KEY,
    ThumbnailPhoto BYTEA,
    ThumbnailPhotoFileName VARCHAR(50),
    LargePhoto BYTEA,
    LargePhotoFileName VARCHAR(50),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Production.ProductProductPhoto (
    ProductID INTEGER NOT NULL REFERENCES Production.Product(ProductID),
    ProductPhotoID INTEGER NOT NULL REFERENCES Production.ProductPhoto(ProductPhotoID),
    "Primary" BOOLEAN NOT NULL DEFAULT FALSE, -- Quoted because Primary is a keyword
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ProductID, ProductPhotoID)
);

CREATE TABLE Production.Culture (
    CultureID CHAR(6) PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_Culture_Name UNIQUE (Name)
);

CREATE TABLE Production.ProductDescription (
    ProductDescriptionID SERIAL PRIMARY KEY,
    Description VARCHAR(400) NOT NULL,
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_ProductDescription_rowguid UNIQUE (rowguid)
);

CREATE TABLE Production.ProductModelProductDescriptionCulture (
    ProductModelID INTEGER NOT NULL REFERENCES Production.ProductModel(ProductModelID),
    ProductDescriptionID INTEGER NOT NULL REFERENCES Production.ProductDescription(ProductDescriptionID),
    CultureID CHAR(6) NOT NULL REFERENCES Production.Culture(CultureID),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ProductModelID, ProductDescriptionID, CultureID)
);

CREATE TABLE Production.Illustration (
    IllustrationID SERIAL PRIMARY KEY,
    Diagram XML,
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Production.ProductModelIllustration (
    ProductModelID INTEGER NOT NULL REFERENCES Production.ProductModel(ProductModelID),
    IllustrationID INTEGER NOT NULL REFERENCES Production.Illustration(IllustrationID),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ProductModelID, IllustrationID)
);

CREATE TABLE Production.Document (
    DocumentNode VARCHAR(255) PRIMARY KEY, -- HIERARCHYID placeholder for path
    DocumentLevel SMALLINT, -- GENERATED ALWAYS AS (HIERARCHYID::GetLevel(DocumentNode)) STORED -- If implementing HIERARCHYID like type
    Title VARCHAR(50) NOT NULL,
    Owner INTEGER NOT NULL REFERENCES HumanResources.Employee(BusinessEntityID),
    FolderFlag BOOLEAN NOT NULL DEFAULT FALSE,
    FileName VARCHAR(400) NOT NULL,
    FileExtension VARCHAR(8),
    Revision CHAR(5) NOT NULL,
    ChangeNumber INTEGER NOT NULL DEFAULT 0,
    Status SMALLINT NOT NULL,
    DocumentSummary TEXT,
    Document BYTEA, -- varbinary(max)
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_Document_rowguid UNIQUE (rowguid),
    CONSTRAINT UQ_Document_FileName_Revision UNIQUE (FileName, Revision)
);

-- This table was shown with DocumentNode as FK, and (ProductID, DocumentNode) as PK in ERD.
-- So DocumentNode is part of the PK and FK to Document.DocumentNode
CREATE TABLE Production.ProductDocument (
    ProductID INTEGER NOT NULL REFERENCES Production.Product(ProductID),
    DocumentNode VARCHAR(255) NOT NULL REFERENCES Production.Document(DocumentNode),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ProductID, DocumentNode)
);


-------------------------------------------------------------------------------
-- Purchasing Schema
-------------------------------------------------------------------------------

CREATE TABLE Purchasing.ShipMethod (
    ShipMethodID SERIAL PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    ShipBase NUMERIC(19,4) NOT NULL DEFAULT 0.00, -- money
    ShipRate NUMERIC(19,4) NOT NULL DEFAULT 0.00, -- money
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_ShipMethod_Name UNIQUE (Name),
    CONSTRAINT UQ_ShipMethod_rowguid UNIQUE (rowguid)
);

CREATE TABLE Purchasing.Vendor (
    BusinessEntityID INTEGER PRIMARY KEY REFERENCES Person.BusinessEntity(BusinessEntityID),
    AccountNumber VARCHAR(15) NOT NULL, -- AccountNumber is U1 in diagram
    Name VARCHAR(50) NOT NULL, -- Name is U2 in diagram
    CreditRating SMALLINT NOT NULL,
    PreferredVendorStatus BOOLEAN NOT NULL DEFAULT TRUE,
    ActiveFlag BOOLEAN NOT NULL DEFAULT TRUE,
    PurchasingWebServiceURL VARCHAR(1024),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_Vendor_AccountNumber UNIQUE (AccountNumber),
    CONSTRAINT UQ_Vendor_Name UNIQUE (Name)
);

CREATE TABLE Purchasing.PurchaseOrderHeader (
    PurchaseOrderID SERIAL PRIMARY KEY,
    RevisionNumber SMALLINT NOT NULL DEFAULT 0,
    Status SMALLINT NOT NULL DEFAULT 1, -- CHECK (Status BETWEEN 1 AND 4),
    EmployeeID INTEGER NOT NULL REFERENCES HumanResources.Employee(BusinessEntityID),
    VendorID INTEGER NOT NULL REFERENCES Purchasing.Vendor(BusinessEntityID),
    ShipMethodID INTEGER NOT NULL REFERENCES Purchasing.ShipMethod(ShipMethodID),
    OrderDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ShipDate TIMESTAMP WITHOUT TIME ZONE,
    SubTotal NUMERIC(19,4) NOT NULL DEFAULT 0.00, -- money
    TaxAmt NUMERIC(19,4) NOT NULL DEFAULT 0.00, -- money
    Freight NUMERIC(19,4) NOT NULL DEFAULT 0.00, -- money
    TotalDue NUMERIC(19,4), -- Computed: (SubTotal + TaxAmt + Freight)
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Purchasing.PurchaseOrderDetail (
    PurchaseOrderID INTEGER NOT NULL REFERENCES Purchasing.PurchaseOrderHeader(PurchaseOrderID),
    PurchaseOrderDetailID SERIAL NOT NULL,
    DueDate TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    OrderQty SMALLINT NOT NULL,
    ProductID INTEGER NOT NULL REFERENCES Production.Product(ProductID),
    UnitPrice NUMERIC(19,4) NOT NULL, -- money
    LineTotal NUMERIC(19,4), -- Computed: (OrderQty * UnitPrice)
    ReceivedQty NUMERIC(8,2) NOT NULL,
    RejectedQty NUMERIC(8,2) NOT NULL,
    StockedQty NUMERIC(9,2), -- Computed: (ReceivedQty - RejectedQty)
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (PurchaseOrderID, PurchaseOrderDetailID)
);


-------------------------------------------------------------------------------
-- Sales Schema
-------------------------------------------------------------------------------

CREATE TABLE Sales.Currency (
    CurrencyCode CHAR(3) PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_Currency_Name UNIQUE (Name)
);

CREATE TABLE Sales.CurrencyRate (
    CurrencyRateID SERIAL PRIMARY KEY,
    CurrencyRateDate TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    FromCurrencyCode CHAR(3) NOT NULL REFERENCES Sales.Currency(CurrencyCode),
    ToCurrencyCode CHAR(3) NOT NULL REFERENCES Sales.Currency(CurrencyCode),
    AverageRate NUMERIC(19,4) NOT NULL, -- money, assuming it's an exchange rate
    EndOfDayRate NUMERIC(19,4) NOT NULL, -- money
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_CurrencyRate_CurrencyRateDate_FromCurrencyCode_ToCurrencyCode UNIQUE (CurrencyRateDate, FromCurrencyCode, ToCurrencyCode)
);

CREATE TABLE Sales.SalesTerritory (
    TerritoryID SERIAL PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    CountryRegionCode VARCHAR(3) NOT NULL REFERENCES Person.CountryRegion(CountryRegionCode),
    "Group" VARCHAR(50) NOT NULL, -- Quoted as Group is a keyword
    SalesYTD NUMERIC(19,4) NOT NULL DEFAULT 0.00, -- money
    SalesLastYear NUMERIC(19,4) NOT NULL DEFAULT 0.00, -- money
    CostYTD NUMERIC(19,4) NOT NULL DEFAULT 0.00, -- money
    CostLastYear NUMERIC(19,4) NOT NULL DEFAULT 0.00, -- money
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_SalesTerritory_Name UNIQUE (Name),
    CONSTRAINT UQ_SalesTerritory_rowguid UNIQUE (rowguid)
);

-- Add FK from Person.StateProvince to Sales.SalesTerritory
ALTER TABLE Person.StateProvince
ADD CONSTRAINT FK_StateProvince_SalesTerritory
FOREIGN KEY (TerritoryID) REFERENCES Sales.SalesTerritory(TerritoryID);

CREATE TABLE Sales.SalesPerson (
    BusinessEntityID INTEGER PRIMARY KEY REFERENCES HumanResources.Employee(BusinessEntityID),
    TerritoryID INTEGER REFERENCES Sales.SalesTerritory(TerritoryID),
    SalesQuota NUMERIC(19,4), -- money
    Bonus NUMERIC(19,4) NOT NULL DEFAULT 0.00, -- money
    CommissionPct NUMERIC(10,4) NOT NULL DEFAULT 0.00, -- smallmoney
    SalesYTD NUMERIC(19,4) NOT NULL DEFAULT 0.00, -- money
    SalesLastYear NUMERIC(19,4) NOT NULL DEFAULT 0.00, -- money
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_SalesPerson_rowguid UNIQUE (rowguid)
);

CREATE TABLE Sales.SalesPersonQuotaHistory (
    BusinessEntityID INTEGER NOT NULL REFERENCES Sales.SalesPerson(BusinessEntityID),
    QuotaDate TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    SalesQuota NUMERIC(19,4) NOT NULL, -- money
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (BusinessEntityID, QuotaDate),
    CONSTRAINT UQ_SalesPersonQuotaHistory_rowguid UNIQUE (rowguid)
);

CREATE TABLE Sales.SalesTerritoryHistory (
    BusinessEntityID INTEGER NOT NULL REFERENCES Sales.SalesPerson(BusinessEntityID),
    TerritoryID INTEGER NOT NULL REFERENCES Sales.SalesTerritory(TerritoryID),
    StartDate TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    EndDate TIMESTAMP WITHOUT TIME ZONE,
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (BusinessEntityID, StartDate, TerritoryID),
    CONSTRAINT UQ_SalesTerritoryHistory_rowguid UNIQUE (rowguid)
);

CREATE TABLE Sales.Store (
    BusinessEntityID INTEGER PRIMARY KEY REFERENCES Person.BusinessEntity(BusinessEntityID),
    Name VARCHAR(50) NOT NULL,
    SalesPersonID INTEGER REFERENCES Sales.SalesPerson(BusinessEntityID),
    Demographics XML,
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_Store_Name UNIQUE (Name),
    CONSTRAINT UQ_Store_rowguid UNIQUE (rowguid)
);

CREATE TABLE Sales.Customer (
    CustomerID SERIAL PRIMARY KEY,
    PersonID INTEGER REFERENCES Person.Person(BusinessEntityID),
    StoreID INTEGER REFERENCES Sales.Store(BusinessEntityID),
    TerritoryID INTEGER REFERENCES Sales.SalesTerritory(TerritoryID),
    AccountNumber VARCHAR(10), -- Computed in MSSQL (VARCHAR(10)) - UQ_Customer_AccountNumber in diagram
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_Customer_AccountNumber UNIQUE (AccountNumber), -- If not computed, needs to be populated
    CONSTRAINT UQ_Customer_rowguid UNIQUE (rowguid),
    CONSTRAINT UQ_Customer_PersonID UNIQUE (PersonID) -- Based on FK2_Customer_Person_PersonID in diagram, and U1 for PersonID
    -- StoreID is U2 on diagram, but means Customer can only be linked to one Store in this way.
    -- CONSTRAINT UQ_Customer_StoreID UNIQUE (StoreID)
);

CREATE TABLE Sales.CreditCard (
    CreditCardID SERIAL PRIMARY KEY,
    CardType VARCHAR(50) NOT NULL,
    CardNumber VARCHAR(25) NOT NULL,
    ExpMonth SMALLINT NOT NULL,
    ExpYear SMALLINT NOT NULL,
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_CreditCard_CardNumber UNIQUE (CardNumber)
);

CREATE TABLE Sales.PersonCreditCard (
    BusinessEntityID INTEGER NOT NULL REFERENCES Person.Person(BusinessEntityID),
    CreditCardID INTEGER NOT NULL REFERENCES Sales.CreditCard(CreditCardID),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (BusinessEntityID, CreditCardID)
);

CREATE TABLE Sales.SalesReason (
    SalesReasonID SERIAL PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    ReasonType VARCHAR(50) NOT NULL,
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_SalesReason_Name UNIQUE (Name)
);

CREATE TABLE Sales.SalesOrderHeader (
    SalesOrderID SERIAL PRIMARY KEY,
    RevisionNumber SMALLINT NOT NULL DEFAULT 0,
    OrderDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    DueDate TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    ShipDate TIMESTAMP WITHOUT TIME ZONE,
    Status SMALLINT NOT NULL DEFAULT 1, -- CHECK (Status BETWEEN 1 AND 6),
    OnlineOrderFlag BOOLEAN NOT NULL DEFAULT TRUE,
    SalesOrderNumber VARCHAR(25), -- Computed in MSSQL (NVARCHAR(25)) - UQ_SalesOrderHeader_SalesOrderNumber in diagram
    PurchaseOrderNumber VARCHAR(25), -- OrderNumber
    AccountNumber VARCHAR(15), -- AccountNumber
    CustomerID INTEGER NOT NULL REFERENCES Sales.Customer(CustomerID),
    SalesPersonID INTEGER REFERENCES Sales.SalesPerson(BusinessEntityID),
    TerritoryID INTEGER REFERENCES Sales.SalesTerritory(TerritoryID),
    BillToAddressID INTEGER NOT NULL REFERENCES Person.Address(AddressID),
    ShipToAddressID INTEGER NOT NULL REFERENCES Person.Address(AddressID),
    ShipMethodID INTEGER NOT NULL REFERENCES Purchasing.ShipMethod(ShipMethodID),
    CreditCardID INTEGER REFERENCES Sales.CreditCard(CreditCardID),
    CreditCardApprovalCode VARCHAR(15),
    CurrencyRateID INTEGER REFERENCES Sales.CurrencyRate(CurrencyRateID),
    SubTotal NUMERIC(19,4) NOT NULL DEFAULT 0.00, -- money
    TaxAmt NUMERIC(19,4) NOT NULL DEFAULT 0.00, -- money
    Freight NUMERIC(19,4) NOT NULL DEFAULT 0.00, -- money
    TotalDue NUMERIC(19,4), -- Computed: (SubTotal + TaxAmt + Freight)
    Comment VARCHAR(128),
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_SalesOrderHeader_SalesOrderNumber UNIQUE (SalesOrderNumber), -- If not computed
    CONSTRAINT UQ_SalesOrderHeader_rowguid UNIQUE (rowguid)
);

CREATE TABLE Sales.SalesOrderDetail (
    SalesOrderID INTEGER NOT NULL REFERENCES Sales.SalesOrderHeader(SalesOrderID),
    SalesOrderDetailID SERIAL NOT NULL,
    CarrierTrackingNumber VARCHAR(25),
    OrderQty SMALLINT NOT NULL,
    ProductID INTEGER NOT NULL REFERENCES Production.Product(ProductID),
    SpecialOfferID INTEGER NOT NULL, -- Part of FK to SpecialOfferProduct along with ProductID
    UnitPrice NUMERIC(19,4) NOT NULL, -- money
    UnitPriceDiscount NUMERIC(19,4) NOT NULL DEFAULT 0.0, -- money
    LineTotal NUMERIC(38,6), -- Computed: ((UnitPrice * (1.0 - UnitPriceDiscount)) * OrderQty)
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (SalesOrderID, SalesOrderDetailID),
    CONSTRAINT UQ_SalesOrderDetail_rowguid UNIQUE (rowguid)
    -- FK to SpecialOfferProduct will be complex, or individual FKs to SpecialOffer and Product.
    -- The diagram implies SpecialOfferID here is a direct reference to SpecialOffer.SpecialOfferID.
);

CREATE TABLE Sales.SpecialOffer (
    SpecialOfferID SERIAL PRIMARY KEY,
    Description VARCHAR(255) NOT NULL,
    DiscountPct NUMERIC(10,4) NOT NULL DEFAULT 0.0, -- smallmoney
    Type VARCHAR(50) NOT NULL,
    Category VARCHAR(50) NOT NULL,
    StartDate TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    EndDate TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    MinQty INTEGER NOT NULL DEFAULT 0,
    MaxQty INTEGER,
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_SpecialOffer_rowguid UNIQUE (rowguid)
);

CREATE TABLE Sales.SpecialOfferProduct (
    SpecialOfferID INTEGER NOT NULL REFERENCES Sales.SpecialOffer(SpecialOfferID),
    ProductID INTEGER NOT NULL REFERENCES Production.Product(ProductID),
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (SpecialOfferID, ProductID),
    CONSTRAINT UQ_SpecialOfferProduct_rowguid UNIQUE (rowguid)
);

-- Add FK from SalesOrderDetail to SpecialOfferProduct
-- This is if SpecialOfferID in SalesOrderDetail actually forms a composite key with ProductID to SpecialOfferProduct
-- However, the ERD seems to suggest SalesOrderDetail.SpecialOfferID is a direct FK to SpecialOffer.SpecialOfferID.
-- If that's the case, the FK should be directly to Sales.SpecialOffer.
ALTER TABLE Sales.SalesOrderDetail
ADD CONSTRAINT FK_SalesOrderDetail_SpecialOffer
FOREIGN KEY (SpecialOfferID) REFERENCES Sales.SpecialOffer(SpecialOfferID);
-- If it were to SpecialOfferProduct:
-- ALTER TABLE Sales.SalesOrderDetail
-- ADD CONSTRAINT FK_SalesOrderDetail_SpecialOfferProduct
-- FOREIGN KEY (SpecialOfferID, ProductID) REFERENCES Sales.SpecialOfferProduct(SpecialOfferID, ProductID);
-- Given ProductID is already a FK to Product, and SpecialOfferID is just one column,
-- the direct FK to SpecialOffer seems more consistent with the ERD's column naming.

CREATE TABLE Sales.SalesOrderHeaderSalesReason (
    SalesOrderID INTEGER NOT NULL REFERENCES Sales.SalesOrderHeader(SalesOrderID),
    SalesReasonID INTEGER NOT NULL REFERENCES Sales.SalesReason(SalesReasonID),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (SalesOrderID, SalesReasonID)
);

CREATE TABLE Sales.SalesTaxRate (
    SalesTaxRateID SERIAL PRIMARY KEY,
    StateProvinceID INTEGER NOT NULL REFERENCES Person.StateProvince(StateProvinceID),
    TaxType SMALLINT NOT NULL, -- CHECK (TaxType BETWEEN 1 AND 3),
    TaxRate NUMERIC(10,4) NOT NULL DEFAULT 0.0, -- smallmoney
    Name VARCHAR(50) NOT NULL,
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT UQ_SalesTaxRate_StateProvinceID_TaxType UNIQUE (StateProvinceID, TaxType),
    CONSTRAINT UQ_SalesTaxRate_rowguid UNIQUE (rowguid)
);

CREATE TABLE Sales.ShoppingCartItem (
    ShoppingCartItemID SERIAL PRIMARY KEY,
    ShoppingCartID VARCHAR(50) NOT NULL,
    Quantity INTEGER NOT NULL DEFAULT 1,
    ProductID INTEGER NOT NULL REFERENCES Production.Product(ProductID),
    DateCreated TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
    -- INDEX IX_ShoppingCartItem_ShoppingCartID_ProductID (ShoppingCartID, ProductID)
);

CREATE TABLE Sales.CountryRegionCurrency (
    CountryRegionCode VARCHAR(3) NOT NULL REFERENCES Person.CountryRegion(CountryRegionCode),
    CurrencyCode CHAR(3) NOT NULL REFERENCES Sales.Currency(CurrencyCode),
    ModifiedDate TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (CountryRegionCode, CurrencyCode)
);
