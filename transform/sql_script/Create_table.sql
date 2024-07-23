USE [testdb]

---- Create a new table called 'Customers' in schema 'dbo'
-- Drop the table if it already exists
DECLARE @sql NVARCHAR(MAX) = N'';
SELECT @sql += 'DROP TABLE IF EXISTS ' + QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME) + ';'
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE';
EXEC sp_executesql @sql;

-- Create tables for DataFactory 
CREATE TABLE Products_Table(
	ProductCardId TEXT NULL,
	ProductCategoryId TEXT NULL,
	ProductDescription TEXT NULL,
	ProductImage TEXT NULL,
	ProductName TEXT NULL,
	ProductPrice TEXT NULL,
	ProductStatus VARCHAR(100) NULL,
	CategoryId TEXT NULL,
	CategoryName TEXT NULL,
);

CREATE TABLE Departments_Table(
	DepartmentId NCHAR(50) NULL, 
	DepartmentName TEXT NULL, 
	Latitude TEXT NULL, 
	Longitude TEXT NULL
);

CREATE TABLE Orders_Table(
	OrderCity TEXT NULL, 
	OrderCountry NCHAR(100) NULL,
	OrderCustomerId TEXT NULL,
	Orderdate NVARCHAR(100) NULL,
	OrderId TEXT NULL,
	OrderItemCardprodId TEXT NULL,
	OrderItemDiscount TEXT NULL,
	OrderItemDiscountRate TEXT NULL,
	OrderItemId TEXT NULL,
	OrderItemProductPrice TEXT NULL,    
	OrderItemProfitRatio TEXT NULL,
	OrderItemQuantity TEXT NULL,
	Sales TEXT NULL,
	OrderItemTotal  NVARCHAR(100) NULL,
	OrderProfitPerOrder NVARCHAR(100) NULL,
	OrderRegion VARCHAR(100) NULL,
	OrderState VARCHAR(100) NULL,
	OrderStatus VARCHAR(50) NULL,
	BenefitPerOrder NVARCHAR(100) NULL, 
	Type TEXT NULL,
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
	Latitude TEXT NULL, 
	Longitude TEXT NULL, 
	SalesPerCustomer NVARCHAR(100) NULL  
);

CREATE TABLE bronze_layer(
	Type TEXT NULL,
	DaysForShipping NVARCHAR(100) NULL,   
	DaysForShipment NVARCHAR(100) NULL,   
	BenefitPerOrder NVARCHAR(100) NULL,   
	SalesPerCustomer TEXT NULL,   
	DeliveryStatus TEXT NULL,
	LateDeliveryRisk TEXT NULL,          
	CategoryId TEXT NULL,
	CategoryName TEXT NULL,
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
	DepartmentId NCHAR(50) NULL, 
	DepartmentName TEXT NULL, 
	Latitude TEXT NULL, 
	Longitude TEXT NULL, 
	Market NCHAR(50) NULL,
	OrderCity TEXT NULL, 
	OrderCountry NCHAR(100) NULL,
	OrderCustomerId TEXT NULL,
	Orderdate TEXT NULL,
	OrderId TEXT NULL,
	OrderItemCardprodId TEXT NULL,
	OrderItemDiscount TEXT NULL,
	OrderItemDiscountRate TEXT NULL,
	OrderItemId TEXT NULL,
	OrderItemProductPrice TEXT NULL,    
	OrderItemProfitRatio TEXT NULL,
	OrderItemQuantity TEXT NULL,
	Sales TEXT NULL,
	OrderItemTotal  NVARCHAR(100),
	OrderProfitPerOrder NVARCHAR(100) NULL,
	OrderRegion VARCHAR(100) NULL,
	OrderState VARCHAR(100) NULL,
	OrderStatus VARCHAR(50) NULL,
	OrderZipcode TEXT NULL,
	ProductCardId TEXT NULL,
	ProductCategoryId TEXT NULL,
	ProductDescription TEXT NULL,
	ProductImage TEXT NULL,
	ProductName TEXT NULL,
	ProductPrice TEXT NULL,
	ProductStatus VARCHAR(100) NULL,
	Shippingdate  NVARCHAR(100) NULL,
	ShippingMode VARCHAR(100) NULL,

);

GO

