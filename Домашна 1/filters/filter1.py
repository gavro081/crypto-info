from .base_filter import Filter
import requests
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

TOTAL_COINS = 1300
BATCH_SIZE = 100
BASE_URL = "https://finance.yahoo.com/markets/crypto/all/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
}

class Filter1(Filter):
    """
    Филтер 1: Автоматски преземете jа листата на наjвредните 1000 активни
    крипто валути
    """
    order = 1

    def __init__(self):
        self.coins = []

    def fetch_page(self, start, count):
        params = {
            "start": start,
            "count": count
        }
        try:
            response = requests.get(BASE_URL, headers=HEADERS, params=params)
            response.raise_for_status()
            return response.text
        except Exception as e:
            print(f"Error fetching page start={start}: {e}")
            return None

    def parse_html(self, html_content):
        try:
            soup = BeautifulSoup(html_content, "html.parser")
            table = soup.find("table")
            if not table:
                return []

            rows = table.find_all("tr")[1:]  # skip header
            if not rows:
                return []

            extracted = []
            valid_quotes = {"USDT", "USDC", "USD", "BTC", "ETH"}
            
            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 10:
                    continue
                
                try:
                    # Extract symbol and validate quote currency
                    symbol = cols[0].text.strip().split("  ")[1]
                    quote = symbol.split("-")[-1]
                    if quote not in valid_quotes:
                        continue
                    
                    name = cols[1].text.strip()
                    market_cap = self._parse_volume(cols[6].text.strip())
                    
                    # Extract 24h volume (cols[8]) - handle B, M, K suffixes
                    volume_text = cols[8].text.strip()
                    volume = self._parse_volume(volume_text)
                    
                    # Extract circulating supply (cols[-3])
                    circ_supply_text = cols[-3].text.strip()
                    circ_supply = self._parse_volume(circ_supply_text)
                    
                    # Extract 52w change % (cols[-2])
                    change_52w_text = cols[-2].text.strip().replace("%", "")
                    change_52w = float(change_52w_text) if change_52w_text and change_52w_text != "--" else None
                    
                    # Apply filters
                    if volume < 100000:
                        continue
                    
                    if circ_supply == 0:
                        continue
                    
                    if change_52w is None or not (-95 < change_52w < 2000):
                        continue
                    
                    extracted.append({
                        "symbol": symbol,
                        "name": name,
                        "change_52w": change_52w,
                        "circulating_supply": circ_supply,
                        "volume": volume,
                        "market_cap": market_cap,
                    })
                    
                except (ValueError, IndexError, AttributeError) as e:
                    continue
            
            return extracted
        except Exception as e:
            print(f"Parse failed: {e}")
            return []
    
    def _parse_volume(self, text):
        """Parse volume/supply strings like '131.179B', '51.205B', '214.428M'"""
        if not text or text == "--":
            return 0

        text = text.strip().upper()
        multiplier = 1

        if text.endswith("B"):
            multiplier = 1_000_000_000
            text = text[:-1]
        elif text.endswith("M"):
            multiplier = 1_000_000
            text = text[:-1]
        elif text.endswith("T"):
            multiplier = 1_000_000_000_000
            text = text[:-1]
        else:
            multiplier = 1_000
            text = text[:-1]

        try:
            return float(text) * multiplier
        except ValueError:
            return 0

    def process_batch(self, start_index):
        html = self.fetch_page(start_index, BATCH_SIZE)
        if html:
            return self.parse_html(html)
        return []

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        print("Starting first filter...")
        start = time.time()
        start_indices = range(0, TOTAL_COINS, BATCH_SIZE)
        
        with ThreadPoolExecutor(max_workers=13) as executor:
            futures = [executor.submit(self.process_batch, start) for start in start_indices]
            
            for future in as_completed(futures):
                try:
                    batch_coins = future.result()
                    if batch_coins:
                        self.coins.extend(batch_coins)
                except Exception as e:
                    print(f"Batch processing failed: {e}")

        end = time.time()
        print(f"Total coins after filtering: {len(self.coins)}")
        print(f"Filter 1 finished in: {end - start:.2f} seconds.")
        
        return pd.DataFrame(self.coins)