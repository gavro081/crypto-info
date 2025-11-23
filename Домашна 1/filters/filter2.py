from filters.helpers import download_one
from database import check_and_update_metadata
from .base_filter import Filter
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict

import pandas as pd
import os
from datetime import date

INTERVALS = ["1d", "5d", "1wk", "1mo"]
OUTPUT_DIR="data"
THREADS=70
os.makedirs(OUTPUT_DIR, exist_ok=True)

class Filter2(Filter):
    """
    Филтер 2: Проверете го последниот датум на достапни податоци
    """
    order = 2

    def load_and_split_coins(self, df: pd.DataFrame) -> List[List[Dict]]:
        data = df.to_dict(orient="records")
        
        if not data:
            return []

        chunk_size = (len(data) + THREADS - 1) // THREADS
        if chunk_size < 1:
            chunk_size = 1

        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

        return chunks


    def process_group(self, group_idx: int, coins: List[Dict]) -> pd.DataFrame:
        group_dfs = []
        for coin in coins:
            df = download_one(coin, period="max")
            if not df.empty:
                group_dfs.append(df)
            time.sleep(0.15)
        
        if group_dfs:
            return pd.concat(group_dfs, ignore_index=True)
        return pd.DataFrame()


    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        print("Starting second filter...")
        start = time.time()
        if 'updated_at' not in df.columns:
            df['updated_at'] = None

        df = check_and_update_metadata(df)

        # only fetch data for coins that are NOT in the database (updated_at is None)
        coins_to_download = df[df['updated_at'].isna()]
        groups = self.load_and_split_coins(coins_to_download)

        print(f"Fetching historic data for {len(coins_to_download)} coins.")

        all_dfs = []
        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = [executor.submit(self.process_group, idx, grp) for idx, grp in enumerate(groups)]
            for f in as_completed(futures):
                res = f.result()
                if not res.empty:
                    all_dfs.append(res)

        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True)
            
            output_path = os.path.join(OUTPUT_DIR, "data_to_add.csv")
            final_df.to_csv(output_path, index=False)

            successful_symbols = final_df['symbol'].unique()
            df.loc[df['symbol'].isin(successful_symbols), 'updated_at'] = date.today()
        else:
            print("No new data downloaded.")
        
        end = time.time()
        print(f"Filter 2 finished in: {end - start:.2f} seconds.")
        
        return df
        