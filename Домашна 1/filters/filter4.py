import time
from database import save_csv_to_db, save_df_to_db
from filters.base_filter import Filter
import pandas as pd

class Filter4(Filter):
    """
    Филтер 4: Пополни база на податоци
    """
    order = 4
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        print("Starting fourth filter...")
        start = time.time()
        save_df_to_db(df, "coins_metadata")
        save_csv_to_db("data/data_to_add.csv", "ohlcv_data", replace=False)
        end = time.time()
        print(f"Filter 4 finished in: {end - start:.2f} seconds.")
        