USE [costco-team3-sqldatabase]

---- Create a new table called 'Customers' in schema 'dbo'
---- Drop the table if it already exists
--IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.bronze_layer') AND type in (N'U'))
--IF OBJECT_ID('dbo.bronze_layer', 'U') IS NOT NULL
--    DROP TABLE dbo.bronze_layer
--GO


-- Create tables for DataFactory 
CREATE TABLE Products_Table(
	ProductCardId TEXT NULL,
	ProductCategoryId TEXT NULL,
	ProductDescription TEXT NULL,
	ProductImage TEXT NULL,
	ProductName TEXT NULL,
	ProductPrice NVARCHAR(100) NULL,
	ProductStatus VARCHAR(100) NULL,
	CategoryId TEXT NULL,
	CategoryName TEXT NULL,
);

CREATE TABLE Departments_Table(
	DepartmentId NCHAR(50) NULL, 
	DepartmentName TEXT NULL, 
	Latitude NVARCHAR(100) NULL, 
	Longitude NVARCHAR(100) NULL
);

CREATE TABLE Orders_Table(
	OrderCity TEXT NULL, 
	OrderCountry NCHAR(100) NULL,
	OrderCustomerId TEXT NULL,
	Orderdate NVARCHAR(100) NULL,
	OrderId TEXT NULL,
	OrderItemCardprodId TEXT NULL,
	OrderItemDiscount NVARCHAR(100) NULL,
	OrderItemDiscountRate NVARCHAR(100) NULL,
	OrderItemId TEXT NULL,
	OrderItemProductPrice TEXT NULL,    
	OrderItemProfitRatio TEXT NULL,
	OrderItemQuantity NVARCHAR(100) NULL,
	Sales NVARCHAR(100) NULL,
	OrderItemTotal  NVARCHAR(100) NULL,
	OrderProfitPerOrder NVARCHAR(100) NULL,
	OrderRegion VARCHAR(100) NULL,
	OrderState VARCHAR(100) NULL,
	OrderStatus VARCHAR(50) NULL,
	BenefitPerOrder NVARCHAR(100) NULL, 
	Type NVARCHAR(100) NULL,
	DaysForShipping NVARCHAR(100) NULL,   
	DaysForShipment NVARCHAR(100) NULL,   
	DeliveryStatus TEXT NULL,
	LateDeliveryRisk TEXT NULL,
	Market NCHAR(50) NULL,
	Shippingdate  NVARCHAR(100) NULL,
	ShippingMode VARCHAR(100) NULL,
);

CREATE TABLE Customer_Table(
	CustomeCity TEXT NULL,
	CustomerCountry TEXT NULL, 
	CustomerEmail TEXT NULL, 
	CustomerFname TEXT NULL,
	CustomerId TEXT NULL,
	CustomerLname TEXT NULL, 
	CustomerPassword TEXT NULL, 
	CustomerSegment NCHAR(50) NULL, 
	CustomerState NCHAR(50) NULL,
	CustomerStreet TEXT NULL, 
	CustomerZipcode NCHAR(50) NULL,
	Latitude NVARCHAR(100) NULL, 
	Longitude NVARCHAR(100) NULL, 
	SalesPerCustomer NVARCHAR(100) NULL  
);

GO

--CREATE TABLE dbo.bronze_layer(
--	Type NVARCHAR(100) NULL,
--	DaysForShipping INT NULL,
--	DaysForShipment INT NULL,
--	BenefitPerOrder DECIMAL NULL,
--	SalesPerCustomer DECIMAL NULL,
--	DeliveryStatus TEXT NULL,
--	LateDeliveryRisk BINARY NULL,
--	CategoryId TEXT NULL,
--	CategoryName TEXT NULL,
--	CustomeCity TEXT NULL,
--	CustomerCountry TEXT NULL,
--	CustomerEmail TEXT NULL,
--	CustomerFname TEXT NULL,
--	CustomerId TEXT NULL,
--	CustomerLname TEXT NULL,
--	CustomerPassword TEXT NULL,
--	CustomerSegment NCHAR(50) NULL,
--	CustomerState NCHAR(50) NULL,
--	CustomerStreet TEXT NULL,
--	CustomerZipcode NCHAR(50) NULL,
--	DepartmentId NCHAR(50) NULL,
--	DepartmentName TEXT NULL,
--	Latitude Decimal(8,6) NULL,
--	Longitude Decimal(9,6) NULL,
--	Market NCHAR(50) NULL,
--	OrderCity TEXT NULL,
--	OrderCountry NCHAR(100) NULL,
--	OrderCustomerId TEXT NULL,
--	Orderdate DATETIME2 NULL,
--	OrderId TEXT NULL,
--	OrderItemCardprodId TEXT NULL,
--	OrderItemDiscount DECIMAL NULL,
--	OrderItemDiscountRate DECIMAL NULL,
--	OrderItemId TEXT NULL,
--	OrderItemProductPrice DECIMAL NULL,
--	OrderItemProfitRatio DECIMAL NULL,
--	OrderItemQuantity INTEGER NULL,
--	Sales DECIMAL NULL,
--	OrderItemTotal  DECIMAL NULL,
--	OrderProfitPerOrder DECIMAL NULL,
--	OrderRegion VARCHAR(100) NULL,
--	OrderState VARCHAR(100) NULL,
--	OrderStatus VARCHAR(50) NULL,
--	ProductCardId TEXT NULL,
--	ProductCategoryId TEXT NULL,
--	ProductDescription TEXT NULL,
--	ProductImage TEXT NULL,
--	ProductName TEXT NULL,
--	ProductPrice DECIMAL NULL,
--	ProductStatus VARCHAR(100) NULL,
--	Shippingdate  DATETIME2 NULL,
--	ShippingMode VARCHAR(100) NULL,

--);