-- insert data from csv file 
USE [testdb]
GO

BULK INSERT dbo.bronze_layer
FROM '/Users/vinhnk1/Desktop/COSCO/prepare_new_ops/data/DataCoSupplyChainDataset.csv'
WITH (
    FIRSTROW = 2, -- Assuming the first row contains headers
    FIELDTERMINATOR = ',', -- Default for CSV
    ROWTERMINATOR = '\n' -- Default row terminator
);
GO


USE [testdb]
SELECT TOP (1000) *   FROM [testdb].[dbo].[bronze_layer]
go
--GO
--EXEC sp_configure 'show advanced options', 1
--RECONFIGURE
--GO
--EXEC sp_configure 'Ad Hoc Distributed Queries', 1
--RECONFIGURE
--EXEC sp_MSset_oledb_prop N'Microsoft.ACE.OLEDB.12.0', N'AllowInProcess', 1   
--EXEC sp_MSset_oledb_prop N'Microsoft.ACE.OLEDB.12.0', N'DynamicParam', 1
--GO

--INSERT INTO dbo.bronze_layer (
--	Type ,
--	DaysForShipping ,   
--	DaysForShipment ,   
--	BenefitPerOrder ,   
--	SalesPerCustomer ,   
--	DeliveryStatus ,
--	LateDeliveryRisk ,          
--	CategoryId ,
--	CategoryName ,
--	CustomeCity ,
--	CustomerCountry , 
--	CustomerEmail , 
--	CustomerFname ,
--	CustomerId ,
--	CustomerLname , 
--	CustomerPassword , 
--	CustomerSegment , 
--	CustomerState ,
--	CustomerStreet , 
--	CustomerZipcode , 
--	DepartmentId , 
--	DepartmentName , 
--	Latitude , 
--	Longitude , 
--	Market ,
--	OrderCity , 
--	OrderCountry ,
--	OrderCustomerId ,
--	Orderdate  ,
--	OrderId ,
--	OrderItemCardprodId ,
--	OrderItemDiscount ,
--	OrderItemDiscountRate ,
--	OrderItemId ,
--	OrderItemProductPrice ,    
--	OrderItemProfitRatio ,
--	OrderItemQuantity ,
--	Sales ,
--	OrderItemTotal  ,
--	OrderProfitPerOrder ,
--	OrderRegion ,
--	OrderState ,
--	OrderStatus  ,
--	ProductCardId ,
--	ProductCategoryId ,
--	ProductDescription ,
--	ProductImage ,
--	ProductName ,
--	ProductPrice ,
--	ProductStatus ,
--	Shippingdate   ,
--	ShippingMode
--)
--SELECT *
--FROM OPENROWSET(
--    'Microsoft.ACE.OLEDB.12.0',
--    'Excel 12.0;Database=C:\Users\Admin\Desktop\Lan\cosco-sql\data\source.xlsx;HDR=YES',
--    'SELECT * FROM [Sheet1$]'
--);

-- check content of table
--select *
--from dbo.bronze_layer

--Go