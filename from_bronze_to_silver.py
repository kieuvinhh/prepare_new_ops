container_name = "costcocontainer"
storage_account = "costcodatastoragegen2"
bronze_folder = "bronzestorage"
silver_folder = "silverstorage"
access_key = ""
spark.conf.set("fs.azure.account.key.{0}.dfs.core.windows.net".format(storage_account), access_key)


from pyspark.sql.functions import col

# Initialize Spark session

# Define the schema mapping
schema = {
    "Type": "string",
    "DaysForShipping": "int",
    "DaysForShipment": "int",
    "BenefitPerOrder": "float",
    "SalesPerCustomer": "float",
    "DeliveryStatus": "string",
    "LateDeliveryRisk": "int",
    "CategoryId": "int",
    "CategoryName": "string",
    "CustomeCity": "string",
    "CustomerCountry": "string",
    "CustomerEmail": "string",
    "CustomerFname": "string",
    "CustomerId": "int",
    "CustomerLname": "string",
    "CustomerPassword": "string",
    "CustomerSegment": "string",
    "CustomerState": "string",
    "Customerstringeet": "string",
    "CustomerZipcode": "int",
    "DepartmentId": "int",
    "DepartmentName": "string",
    "Latitude": "float",
    "Longitude": "float",
    "Market": "string",
    "OrderCity": "string",
    "OrderCountry": "string",
    "OrderCustomerId": "int",
    "Orderdate": "string",
    "OrderId": "int",
    "OrderItemCardprodId": "int",
    "OrderItemDiscount": "float",
    "OrderItemDiscountRate": "float",
    "OrderItemId": "int",
    "OrderItemProductPrice": "float",
    "OrderItemProfitRatio": "float",
    "OrderItemQuantity": "int",
    "Sales": "float",
    "OrderItemTotal": "float",
    "OrderProfitPerOrder": "float",
    "OrderRegion": "string",
    "OrderState": "string",
    "OrderStatus": "string",
    "OrderZipcode": "string",
    "ProductCardId": "int",
    "ProductCategoryId": "string",
    "ProductDescription": "string",
    "ProductImage": "string",
    "ProductName": "string",
    "ProductPrice": "string",
    "ProductStatus": "string",
    "Shippingdate": "string",
    "ShippingMode": "string"
}

# File paths
file_paths = [
    'dbo.Orders_Table.parquet',
    'dbo.Customer_Table.parquet',
    'dbo.Departments_Table.parquet',
    'dbo.Products_Table.parquet'
]

# Process each file

for file_path in file_paths:
    # Load the Parquet file into a DataFrame
    df = spark.read.parquet(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{bronze_folder}/{file_path}")
    # Change data types according to the schema
    for column, dtype in schema.items():
        if column in df.columns:
            if dtype == "int":
                df = df.withColumn(column, col(column).cast("integer"))
            elif dtype == "float":
                df = df.withColumn(column, col(column).cast("float"))
            else:
                df = df.withColumn(column, col(column).cast(dtype))
    new_file_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{silver_folder}/{file_path}"
    df.write.mode("overwrite").parquet(new_file_path)

    print(f"Processed and saved: {new_file_path}")