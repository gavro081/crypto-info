import os
import pandas as pd
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv
import io

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "crypto_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_PORT = os.getenv("DB_PORT", "5432")

def get_engine():
    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(connection_string)

def save_df_to_db(df: pd.DataFrame, table_name: str, replace: bool = True, add_id: bool = True):
    engine = get_engine()
    try:
        print(f"Saving {len(df)} rows to table '{table_name}'...")
        
        # add auto-incrementing ID column if requested
        if add_id and 'id' not in df.columns:
            df = df.copy()
            df.insert(0, 'id', range(1, len(df) + 1))
        
        # use postgres COPY for large datasets (> 700k rows)
        if len(df) > 700000:
            
            if replace:
                df.head(0).to_sql(table_name, engine, if_exists='replace', index=False)
            else:
                df.head(0).to_sql(table_name, engine, if_exists='append', index=False)

            output = io.StringIO()
            df.to_csv(output, index=False, header=False)
            output.seek(0)

            with engine.connect() as connection:
                dbapi_conn = connection.connection
                with dbapi_conn.cursor() as cursor:
                    cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH (FORMAT CSV)", output)
                dbapi_conn.commit()
            
        else:
            # standard to_sql for smaller datasets
            if_exists = "replace" if replace else "append"
                
            df.to_sql(table_name, engine, if_exists=if_exists, index=False, chunksize=10000)
            
    except Exception as e:
        print(f"Error saving to database: {e}")

def save_csv_to_db(csv_path: str, table_name: str, replace: bool = True):
    if not os.path.exists(csv_path):
        print(f"File not found: {csv_path}")
        return

    try:
        df = pd.read_csv(csv_path)
        save_df_to_db(df, table_name, replace=replace)
    except Exception as e:
        print(f"Error processing CSV: {e}")

def check_and_update_metadata(df: pd.DataFrame) -> pd.DataFrame:
    engine = get_engine()
    inspector = inspect(engine)
    
    if not inspector.has_table("coins_metadata"):
        df['updated_at'] = None
        return df
    
    try:
        query = "SELECT symbol, updated_at FROM coins_metadata"
        db_df = pd.read_sql(query, engine)
        
        db_map = dict(zip(db_df['symbol'], db_df['updated_at']))
        
        df['updated_at'] = df['symbol'].map(db_map)
        
        return df
    except Exception as e:
        print(f"Error checking metadata: {e}")
        df['updated_at'] = None
        return df
