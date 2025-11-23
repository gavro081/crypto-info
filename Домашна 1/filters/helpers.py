from ast import Dict
import pandas as pd
import yfinance as yf
from typing import Literal
import time
from datetime import date
import logging

# Silence yfinance logger
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

INTERVALS = ["1d", "5d", "1wk", "1mo"]

def download_one(coin: Dict, period: Literal["max", "1m"]) -> pd.DataFrame:
    ticker = coin["symbol"]
    name = coin["name"]
    
    for attempt in range(3):
        for interval in INTERVALS:
            try:
                dat = yf.Ticker(ticker).history(
                    period=period,
                    interval=interval,
                    auto_adjust=False,
                    actions=False
                )
                
                if dat.empty:
                    continue
                
                df = dat.copy()
                
                if "Adj Close" not in df.columns and "Close" in df.columns:
                    df["Adj Close"] = df["Close"]

                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.droplevel(1)

                df = df.reset_index()
                if "Date" in df.columns:
                    df["date"] = df["Date"].dt.date
                    df = df.drop(columns=["Date"])

                updated_at = coin.get("updated_at")
                if pd.notna(updated_at):
                    if isinstance(updated_at, str):
                        try:
                            updated_at = pd.to_datetime(updated_at).date()
                        except:
                            pass
                    elif isinstance(updated_at, pd.Timestamp):
                        updated_at = updated_at.date()
                    
                    if isinstance(updated_at, date):
                        df = df[df["date"] > updated_at]
                        
                df = df.rename(columns={
                    "Open": "open", "High": "high", "Low": "low",
                    "Close": "close", "Volume": "volume", "Adj Close": "Adj close"
                })

                if len(df.columns) != len(set(df.columns)):
                    seen = {}
                    new_cols = []
                    for c in df.columns:
                        if c in seen:
                            new_cols.append(f"{c}_{seen[c]}")
                            seen[c] += 1
                        else:
                            new_cols.append(c)
                            seen[c] = 1
                    df.columns = new_cols

                desired_columns = ["Adj close", "close", "high", "low", "open", "volume", "date"]
                existing_columns = [c for c in desired_columns if c in df.columns]
                
                df = df[existing_columns]

                df["symbol"] = ticker
                df["name"] = name

                price_cols = ["open", "high", "low", "close", "Adj close"]
                existing_price_cols = [c for c in price_cols if c in df.columns]
                if existing_price_cols:
                    df[existing_price_cols] = df[existing_price_cols]
                
                if "volume" in df.columns:
                    df["volume"] = df["volume"].fillna(0).astype("int64")

                df = df.drop_duplicates(subset=["date"])
                df = df.sort_values("date").dropna(subset=existing_price_cols).reset_index(drop=True)
                
                df["date"] = df["date"].astype(str)

                return df

            except Exception as e:
                msg = str(e).lower()
                if "401" in msg and attempt < 2:
                    time.sleep(1)
                    break
                if "max must be" in msg or "invalid interval" in msg:
                    continue
                pass
        else:
            continue
    return pd.DataFrame()

