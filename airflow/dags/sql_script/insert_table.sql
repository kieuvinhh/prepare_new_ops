USE [costcodatabase]

-- BULK INSERT dbo.bronze_layer
-- from '/Users/vinhnk1/Desktop/COSCO/prepare_new_ops/data/DataCoSupplyChainDataset.csv'
-- WITH ( FORMAT = 'CSV');

INSERT INTO dbo.Products_Table (
	ProductCardId,
	ProductCategoryId,
	ProductDescription,
	ProductImage,
	ProductName,
	ProductPrice,
	ProductStatus,
	CategoryId,
	CategoryName)
SELECT 
	ProductCardId,
	ProductCategoryId,
	ProductDescription,
	ProductImage,
	ProductName,
	ProductPrice,
	ProductStatus,
	CategoryId,
	CategoryName
FROM dbo.bronze_layer

INSERT INTO dbo.Departments_Table (
	DepartmentId, 
	DepartmentName, 
	Latitude, 
	Longitude)
SELECT 
	DepartmentId, 
	DepartmentName, 
	Latitude, 
	Longitude
FROM dbo.bronze_layer

INSERT INTO dbo.Orders_Table (
	OrderCity, 
	OrderCountry,
	OrderCustomerId,
	Orderdate,
	OrderId,
	OrderItemCardprodId,
	OrderItemDiscount,
	OrderItemDiscountRate,
	OrderItemId,
	OrderItemProductPrice,    
	OrderItemProfitRatio,
	OrderItemQuantity,
	Sales,
	OrderItemTotal,
	OrderProfitPerOrder,
	OrderRegion,
	OrderState,
	OrderStatus,
	BenefitPerOrder, 
	Type,
	DaysForShipping,   
	DaysForShipment,   
	DeliveryStatus,
	LateDeliveryRisk,
	Market,
	Shippingdate,
	ShippingMode)
SELECT 
	OrderCity, 
	OrderCountry,
	OrderCustomerId,
	Orderdate,
	OrderId,
	OrderItemCardprodId,
	OrderItemDiscount,
	OrderItemDiscountRate,
	OrderItemId,
	OrderItemProductPrice,    
	OrderItemProfitRatio,
	OrderItemQuantity,
	Sales,
	OrderItemTotal,
	OrderProfitPerOrder,
	OrderRegion,
	OrderState,
	OrderStatus,
	BenefitPerOrder, 
	Type,
	DaysForShipping,   
	DaysForShipment,   
	DeliveryStatus,
	LateDeliveryRisk,
	Market,
	Shippingdate,
	ShippingMode
FROM dbo.bronze_layer


INSERT INTO dbo.Customer_Table (
	CustomeCity,
	CustomerCountry, 
	CustomerEmail, 
	CustomerFname,
	CustomerId,
	CustomerLname, 
	CustomerPassword, 
	CustomerSegment, 
	CustomerState,
	CustomerStreet, 
	CustomerZipcode,
	Latitude, 
	Longitude, 
	SalesPerCustomer)
SELECT 
	CustomeCity,
	CustomerCountry, 
	CustomerEmail, 
	CustomerFname,
	CustomerId,
	CustomerLname, 
	CustomerPassword, 
	CustomerSegment, 
	CustomerState,
	CustomerStreet, 
	CustomerZipcode,
	Latitude, 
	Longitude, 
	SalesPerCustomer
FROM dbo.bronze_layer

go