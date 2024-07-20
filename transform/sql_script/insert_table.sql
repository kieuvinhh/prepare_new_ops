USE costco;

INSERT INTO Products_Table (
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
FROM bronze_layer;

INSERT INTO Departments_Table (
	DepartmentId, 
	DepartmentName, 
	Latitude, 
	Longitude)
SELECT 
	DepartmentId, 
	DepartmentName, 
	Latitude, 
	Longitude
FROM bronze_layer;

INSERT INTO Orders_Table (
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
FROM bronze_layer;


INSERT INTO Customer_Table (
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
FROM bronze_layer;

show columns from bronze_layer;