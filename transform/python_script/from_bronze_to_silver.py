from pyspark.sql import SparkSession
import pandas as pd
import pyarrow

# Initialize Spark session

# Define the schema mapping
schema = {
    "Type": "str",
    "DaysForShipping": "int",
    "DaysForShipment": "int",
    "BenefitPerOrder": "float",
    "SalesPerCustomer": "float",
    "DeliveryStatus": "str",
    "LateDeliveryRisk": "int",
    "CategoryId": "int",
    "CategoryName": "str",
    "CustomeCity": "str",
    "CustomerCountry": "str",
    "CustomerEmail": "str",
    "CustomerFname": "str",
    "CustomerId": "int",
    "CustomerLname": "str",
    "CustomerPassword": "str",
    "CustomerSegment": "str",
    "CustomerState": "str",
    "CustomerStreet": "str",
    "CustomerZipcode": "int",
    "DepartmentId": "int",
    "DepartmentName": "str",
    "Latitude": "float",
    "Longitude": "float",
    "Market": "str",
    "OrderCity": "str",
    "OrderCountry": "str",
    "OrderCustomerId": "int",
    "Orderdate": "str",
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
    "OrderRegion": "str",
    "OrderState": "str",
    "OrderStatus": "str",
    "OrderZipcode": "str",
    "ProductCardId": "int",
    "ProductCategoryId": "str",
    "ProductDescription": "str",
    "ProductImage": "str",
    "ProductName": "str",
    "ProductPrice": "str",
    "ProductStatus": "str",
    "Shippingdate": "str",
    "ShippingMode": "str"
}

# File paths
file_paths = [
    'Orders_Table.parquet',
    'Customer_Table.parquet',
    'Departments_Table.parquet',
    'Products_Table.parquet'
]

# Process each file
for file_path in file_paths:
    # Load the Parquet file into a DataFrame
    df = pd.read_parquet(f"data/bronze/{file_path}")

    # Change data types according to the schema
    for column, dtype in schema.items():
        if column in df.columns:
            if dtype == "int":
                df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype(int)
            elif dtype == "float":
                df[column] = pd.to_numeric(df[column], errors='coerce').astype(float)
            else:
                df[column] = df[column].astype(dtype)
    new_file_path = f"data/silver/{file_path}"
    df.to_parquet(new_file_path)

    print(f"Processed and saved: {new_file_path}")