
CREATE TABLE IF NOT EXISTS dim_customer_detail_table (
    CustomerId STRING NOT NULL,
    CustomerZipcode INT,
    CustomerFname STRING,
    CustomerEmail STRING,
    CustomerLname STRING,
    CustomerPassword STRING,
    CustomerSegment STRING
)
USING DELTA
OPTIONS (
  path "dbfs:/mnt/costcostoragecontainer/goldstorage/dim_customer_detail_table.parquet"
);


CREATE TABLE IF NOT EXISTS dim_customer_location_table (
    CustomerZipcode STRING NOT NULL,
    CustomerState STRING,
    CustomerStreet STRING
)
USING DELTA
OPTIONS (
  path "dbfs:/mnt/costcostoragecontainer/goldstorage/dim_customer_location_table.parquet"
);

CREATE TABLE IF NOT EXISTS dim_order_detail_table (
    OrderId STRING NOT NULL,
    OrderItemId INT,
    OrderCountry STRING,
    OrderCity STRING,
    Market STRING,
    OrderRegion STRING,
    OrderState STRING,
    OrderStatus STRING
)
USING DELTA
OPTIONS (
  path "dbfs:/mnt/costcostoragecontainer/goldstorage/dim_order_detail_table.parquet"
);

CREATE TABLE IF NOT EXISTS dim_order_item_table (
    OrderItemId STRING NOT NULL,
    OrderItemProductPrice FLOAT,
    OrderItemCardprodId INT NOT NULL,
    OrderItemDiscountRate FLOAT,
    OrderItemProfitRatio FLOAT,
    OrderItemQuantity INT
)
USING DELTA
OPTIONS (
  path "dbfs:/mnt/costcostoragecontainer/goldstorage/dim_order_item_table.parquet"
);

CREATE TABLE IF NOT EXISTS dim_shipping_table (
    OrderId STRING NOT NULL,
    DaysForShipping INT,
    DaysForShipment INT,
    DeliveryStatus STRING,
    Shippingdate TIMESTAMP
)
USING DELTA
OPTIONS (
  path "dbfs:/mnt/costcostoragecontainer/goldstorage/dim_shipping_table.parquet"
);