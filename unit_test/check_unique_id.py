import unittest

import pandas as pd
import os


class DataframeTest(unittest.TestCase):
    folder_gold_path = os.getenv('FOLDER_GOLD_PATH', './data/gold')

    def test_unique_id(self):
        parquet_files = {
            "dim_order_item_table.parquet": "OrderItemId",
            "dim_order_detail_table.parquet": "OrderId",
            "dim_shipping_table.parquet": "OrderId",
            "dim_customer_detail_table.parquet": "CustomerId",
            "dim_customer_location_table.parquet": "CustomerZipcode",
        }
        for file_name, subset in parquet_files.items():
            df = pd.read_parquet(f"{self.folder_gold_path}/{file_name}")
            self.assertEqual(df[subset].is_unique, True, f"Duplicate values found in {file_name}")
