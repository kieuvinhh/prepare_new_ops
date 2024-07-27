# from pyspark.sql.functions import col
# from pyspark.sql.session import SparkSession
#
# spark = SparkSession.builder.appName("from_bronze_to_sivler").getOrCreate()
#
# container_name = "costcocontainer"
# storage_account = "costcodatastoragegen2"
# bronze_folder = "bronzestorage"
# silver_folder = "silverstorage"
# access_key = ""
# spark.conf.set("fs.azure.account.key.{0}.dfs.core.windows.net".format(storage_account), access_key)

# Initialize Spark session
import pandas as pd

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
    'Orders_Table.parquet',
    'Customer_Table.parquet',
    'Departments_Table.parquet',
    'Products_Table.parquet'
]


# Process each file
def process():
    for file_path in file_paths:
        # Load the Parquet file into a DataFrame
        df = pd.read_parquet(f"/opt/airflow/bronze/{file_path}")
        # Change data types according to the schema
        for column, dtype in schema.items():
            if column in df.columns:
                if dtype == "int":
                    df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype(int)
                elif dtype == "float":
                    df[column] = pd.to_numeric(df[column], errors='coerce').astype(float)
                else:
                    df[column] = df[column].astype(dtype)
        df.to_parquet(f"/opt/airflow/silver/{file_path}")
        print(f"Processed and saved:" + f"/opt/airflow/silver/{file_path}")


if __name__ == '__main__':
    process()
