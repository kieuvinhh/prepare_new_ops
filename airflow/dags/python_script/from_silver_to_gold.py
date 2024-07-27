import pandas as pd

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
    "Orderdate": "datetime",
    "Sales": "float"
}

dim_shipping_table_schema = {
    "OrderId": "string",
    "DaysForShipping": "int",
    "DaysForShipment": "int",
    "DeliveryStatus": "string",
    "Shippingdate": "string"
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


def transform_dim_customer(file_path_input, schema, file_path_output, id_col):
    output_folder = "/opt/airflow/gold"
    df = pd.read_parquet(f"/opt/airflow/silver/{file_path_input}")
    df = apply_schema(df, schema).drop_duplicates(subset=[id_col])
    df = df[schema.keys()]
    df.to_parquet(f"{output_folder}/{file_path_output}", engine='pyarrow', index=False)
    print(f"Processed and saved: " + f"{output_folder}/{file_path_output}")


def process():
    process_fact()
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


def process_fact():
    # Define schema dictionaries
    fact_order_table_schema = {
        "OrderId": "string",
        "OrderCustomerId": "string",
        "OrderItemTotal": "float",
        "OrderProfitPerOrder": "float",
        "Orderdate": "string",
        "Sales": "float"
    }

    fact_product_table_schema = {
        "DepartmentId": "string",
        "ProductCardId": "string"
    }

    # Function to apply schema
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

    # Read the CSV file
    csv_file_path = '/opt/airflow/data/DataCoSupplyChainDataset.csv'  # Update this path
    encoding = 'latin1'  # Use the appropriate encoding

    try:
        df = pd.read_csv(csv_file_path, encoding=encoding)
    except UnicodeDecodeError as e:
        print(f"Encoding error: {e}")
        # Optionally, you can handle the error or try a different encoding here

    # Rename columns for fact order table
    new_column_names_order = {
        "Order Id": "OrderId",
        "Order Customer Id": "OrderCustomerId",
        "Order Item Total": "OrderItemTotal",
        "Order Profit Per Order": "OrderProfitPerOrder",
        "shipping date (DateOrders)": "Orderdate",
        "Sales": "Sales",
    }

    # Select and rename columns for fact order table
    df_order_fact = df[list(new_column_names_order.keys())].rename(columns=new_column_names_order)
    df_order_fact = apply_schema(df_order_fact, fact_order_table_schema).drop_duplicates(subset=["OrderId"])
    df_order_fact = df_order_fact.drop_duplicates(subset=['OrderId'])

    # Rename columns for fact product table
    new_column_names_product = {
        "Department Id": "DepartmentId",
        "Product Card Id": "ProductCardId",
    }

    # Select and rename columns for fact product table
    df_product_fact = df[list(new_column_names_product.keys())].rename(columns=new_column_names_product)
    df_product_fact = apply_schema(df_product_fact, fact_product_table_schema).drop_duplicates()
    df_product_fact = df_product_fact.drop_duplicates(subset=['DepartmentId', 'ProductCardId'])
    output_folder = "/opt/airflow/gold"  # Update this path

    # Save the modified DataFrame to a Parquet file
    parquet_file_path_order = f'{output_folder}/fact_order_table.parquet'
    df_order_fact.to_parquet(parquet_file_path_order, engine='pyarrow', index=False)

    parquet_file_path_product = f'{output_folder}/fact_product_table.parquet'
    df_product_fact.to_parquet(parquet_file_path_product, engine='pyarrow', index=False)

    print(f"Processed and saved: {parquet_file_path_order}, {parquet_file_path_product}")

if __name__ == '__main__':
    process()
