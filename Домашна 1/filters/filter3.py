from filters.helpers import download_one
from .base_filter import Filter
import pandas as pd
import os
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict
import time

OUTPUT_DIR = "data"
THREADS = 10
COINS_PER_THREAD = 100

class Filter3(Filter):
    """
    Филтер 3: Пополнете ги податоците што недостасуваат
    """
    order = 3

    def process_group(self, group_idx: int, coins: List[Dict]) -> pd.DataFrame:
        group_dfs = []
        for coin in coins:
            
            updated_at = coin.get('updated_at')
            period = "max"
            
            if pd.notna(updated_at):
                if isinstance(updated_at, str):
                    try:
                        updated_at = pd.to_datetime(updated_at).date()
                    except:
                        pass 
                
                if isinstance(updated_at, date):
                    delta = date.today() - updated_at
                    if delta.days < 30:
                        period = "1mo"
            
            df = download_one(coin, period=period)
            if not df.empty:
                group_dfs.append(df)
            time.sleep(0.15)
        
        if group_dfs:
            return pd.concat(group_dfs, ignore_index=True)
        return pd.DataFrame()

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        print("Starting third filter...")
        start = time.time()
        today = date.today()
        
        if 'updated_at' not in df.columns:
            filtered_df = df.copy()
        else:
            filtered_df = df[df['updated_at'] != today]
        
        if filtered_df.empty:
            print("All coins are up to date. No need for filter 3.")
            return df

        print(f"Found {len(filtered_df)} coins to update.")

        data = filtered_df.to_dict(orient="records")
        
        chunk_size = COINS_PER_THREAD
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
        chunks = chunks[:THREADS]
        
        all_dfs = []
        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = [executor.submit(self.process_group, idx, grp) for idx, grp in enumerate(chunks)]
            for f in as_completed(futures):
                res = f.result()
                if not res.empty:
                    all_dfs.append(res)
        
        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True)
            
            output_path = os.path.join(OUTPUT_DIR, "data_to_add.csv")
            header = not os.path.exists(output_path)
            final_df.to_csv(output_path, mode='a', header=header, index=False)
            
            processed_symbols = final_df['symbol'].unique()
            df.loc[df['symbol'].isin(processed_symbols), 'updated_at'] = today
            
        else:
            print("No new data downloaded in Filter 3.")

        end = time.time()
        print(f"Filter 3 finished in: {end - start:.2f} seconds.")
        return df