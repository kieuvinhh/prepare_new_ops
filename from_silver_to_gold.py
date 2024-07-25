container_name = "costcocontainer"
storage_account = "costcodatastoragegen2"
access_key = ""

spark.conf.set("fs.azure.account.key.{0}.dfs.core.windows.net".format(storage_account), access_key)

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import col

# Define the schema for each table
dim_order_item_table_schema = {
    "OrderItemId": "string",
    "OrderItemProductPrice": "float",
    "OrderItemCardprodId": "int",
    "OrderItemDiscountRate": "float",
    "OrderItemProfitRatio": "float",
    "OrderItemQuantity": "int"
}

dim_order_detail_table_schema = {
    "OrderId": "string",
    "OrderItemId": "int",
    "OrderCountry": "string",
    "OrderCity": "string",
    "Market": "string",
    "OrderRegion": "string",
    "OrderState": "string",
    "OrderStatus": "string"

}

fact_order_table_schema = {
    "OrderId": "string",
    "OrderCustomer_Id": "int",
    "OrderItem_Total": "float",
    "OrderProfitPerOrder": "float",
    "Orderdate": "string",  # Assuming date is stored as text, you can change it to datetime if needed
    "Sales": "float"
}

dim_shipping_table_schema = {
    "OrderId": "string",
    "DaysForShipping": "int",
    "DaysForShipment": "int",
    "DeliveryStatus": "string",
    "Shippingdate": TimestampType()
}

dim_customer_detail_table_schema = {
    "CustomerId": "string",
    "CustomerZipcode": "int",
    "CustomerFname": "string",
    "CustomerEmail": "string",
    "CustomerLname": "string",
    "CustomerPassword": "string",
    "CustomerSegment": "string"
}

dim_customer_location_table_schema = {
    "CustomerZipcode": "int",
    "CustomerState": "string",
    "CustomerStreet": "string"
}

dim_date_table_schema = {
    "Date": "int",
    "Month": "int",
    "Day": "int",
    "Day_of_week": "int",
    "Day_of_month": "int",
    "Year": "int"
}


# Function to change data types according to schema
def apply_schema(df, schema):
    for column, dtype in schema.items():
        if column in df.columns:
            if dtype == "int":
                df = df.withColumn(column, col(column).cast("integer"))
                df = df.fillna(0, subset=[column])
            elif dtype == "float":
                df = df.withColumn(column, col(column).cast("float"))
            else:
                df = df.withColumn(column, col(column).cast(dtype))
    return df


# Example of loading, applying schema, and saving Parquet files
# Replace with your actual Parquet file paths
parquet_files = {
    "dim_order_item_table.parquet": dim_order_item_table_schema,
    "dim_order_detail_table.parquet": dim_order_detail_table_schema,
    "fact_order_table.parquet": fact_order_table_schema,
    "dim_shipping_table.parquet": dim_shipping_table_schema,
    "dim_customer_detail_table.parquet": dim_customer_detail_table_schema,
    "dim_customer_location_table.parquet": dim_customer_location_table_schema,
    "dim_date_table.parquet": dim_date_table_schema
}


def transform_dim_customer(folder_path, schema, parquet_files, id_col):
    df =  spark.read.parquet(f"abfss://costcocontainer@costcodatastoragegen2.dfs.core.windows.net/silverstorage/{folder_path}")
    df = apply_schema(df, schema).drop_duplicates(subset=[id_col])
    df = df.select([col for col in schema.keys()])
    new_file_path = f"abfss://costcocontainer@costcodatastoragegen2.dfs.core.windows.net/goldstorage/{parquet_files}"
    df.write.mode("overwrite").parquet(new_file_path)
    print(f"Processed and saved: {new_file_path}")


transform_dim_customer("dbo.Customer_Table.parquet",
                       dim_customer_detail_table_schema,
                       "dim_customer_detail_table.parquet",
                       "CustomerId")
transform_dim_customer("dbo.Customer_Table.parquet",
                       dim_customer_location_table_schema,
                       "dim_customer_location_table.parquet",
                       "CustomerZipcode")
transform_dim_customer("dbo.Orders_Table.parquet",
                       dim_order_item_table_schema,
                       "dim_order_item_table.parquet",
                       "OrderId")
transform_dim_customer("dbo.Orders_Table.parquet",
                       dim_order_detail_table_schema,
                       "dim_order_detail_table.parquet",
                       "OrderId")
transform_dim_customer("dbo.Orders_Table.parquet",
                       dim_shipping_table_schema, "dim_shipping_table.parquet",
                       "OrderId")
# transform_dim_customer("Orders_Table.parquet",
#                        dim_date_table_schema,
#                        "dim_date_table.parquet",
#                        "OrderId")
