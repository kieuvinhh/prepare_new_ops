import pandas as pd

# Define schema dictionaries
fact_order_table_schema = {
    "OrderId": "int",
    "OrderCustomerId": "int",
    "OrderItemTotal": "float",
    "OrderProfitPerOrder": "float",
    "Orderdate": "str",
    "Sales": "float"
}

fact_product_table_schema = {
    "Longitude": "float",
    "Latitude": "float",
    "DepartmentId": "int",
    "ProductCardId": "int"
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
csv_file_path = '/Users/vinhnk1/Desktop/COSCO/COSTCO/data/DataCoSupplyChainDataset.csv'  # Replace with the actual path
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
df_order_fact = apply_schema(df_order_fact, fact_order_table_schema).drop_duplicates()

# Rename columns for fact product table
new_column_names_product = {
    "Longitude": "Longitude",
    "Latitude": "Latitude",
    "Department Id": "DepartmentId",
    "Product Card Id": "ProductCardId",
}

# Select and rename columns for fact product table
df_product_fact = df[list(new_column_names_product.keys())].rename(columns=new_column_names_product)
df_product_fact = apply_schema(df_product_fact, fact_product_table_schema).drop_duplicates()

# Save the modified DataFrame to a Parquet file
parquet_file_path_order = '/Users/vinhnk1/Desktop/COSCO/COSTCO/data/gold/fact_order_table.parquet'  # Replace with your desired Parquet file path
df_order_fact.to_parquet(parquet_file_path_order, engine='pyarrow', index=False)

parquet_file_path_product = '/Users/vinhnk1/Desktop/COSCO/COSTCO/data/gold/fact_product_table.parquet'  # Replace with your desired Parquet file path
df_product_fact.to_parquet(parquet_file_path_product, engine='pyarrow', index=False)

print(f"Processed and saved: {parquet_file_path_order}, {parquet_file_path_product}")
