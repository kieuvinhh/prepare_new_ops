import pandas as pd
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Define the schema for each table
dim_order_item_table_schema = {
    "OrderItemId": "str",
    "OrderItemProductPrice": "float",
    "OrderItemCardprodId": "int",
    "OrderItemDiscountRate": "float",
    "OrderItemProfitRatio": "float",
    "OrderItemQuantity": "int"
}

dim_order_detail_table_schema = {
    "OrderId": "str",
    "OrderItemId": "int",
    "OrderCountry": "str",
    "OrderCity": "str",
    "Market": "str",
    "OrderRegion": "str",
    "OrderState": "str",
    "OrderStatus": "str"

}

fact_order_table_schema = {
    "OrderId": "str",
    "OrderCustomer_Id": "int",
    "OrderItem_Total": "float",
    "OrderProfitPerOrder": "float",
    "Orderdate": "str",  # Assuming date is stored as text, you can change it to datetime if needed
    "Sales": "float"
}

dim_shipping_table_schema = {
    "OrderId": "str",
    "DaysForShipping": "int",
    "DaysForShipment": "int",
    "DeliveryStatus": "str",
    "Shippingdate": "datetime64[ns]"
}

dim_customer_detail_table_schema = {
    "CustomerId": "str",
    "CustomerZipcode": "int",
    "CustomerFname": "str",
    "CustomerEmail": "str",
    "CustomerLname": "str",
    "CustomerPassword": "str",
    "CustomerSegment": "str"
}

dim_customer_location_table_schema = {
    "CustomerZipcode": "int",
    "CustomerState": "str",
    "CustomerStreet": "str"
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
                df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype(int)
            elif dtype == "float":
                df[column] = pd.to_numeric(df[column], errors='coerce').astype(float)
            else:
                df[column] = df[column].astype(dtype)
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


def transform_dim_customer(fileName, schema, file_path, id_col):
    df = pd.read_parquet(f"data/silver/{fileName}", engine="pyarrow")
    df = apply_schema(df, schema).drop_duplicates(subset=[id_col])
    df = df[schema.keys()]
    new_file_path = f"data/gold/{file_path}"
    df.to_parquet(new_file_path, engine='pyarrow', index=False)
    print(f"Processed and saved: {new_file_path}")


transform_dim_customer("Customer_Table.parquet",
                       dim_customer_detail_table_schema,
                       "dim_customer_detail_table.parquet",
                       "CustomerId")
transform_dim_customer("Customer_Table.parquet",
                       dim_customer_location_table_schema,
                       "dim_customer_location_table.parquet",
                       "CustomerZipcode")
transform_dim_customer("Orders_Table.parquet",
                       dim_order_item_table_schema,
                       "dim_order_item_table.parquet",
                       "OrderId")
transform_dim_customer("Orders_Table.parquet",
                       dim_order_detail_table_schema,
                       "dim_order_detail_table.parquet",
                       "OrderId")
transform_dim_customer("Orders_Table.parquet",
                       dim_shipping_table_schema, "dim_shipping_table.parquet",
                       "OrderId")
# transform_dim_customer("Orders_Table.parquet",
#                        dim_date_table_schema,
#                        "dim_date_table.parquet",
#                        "OrderId")
