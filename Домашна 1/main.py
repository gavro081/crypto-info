import multiprocessing
import time
import pandas as pd
from filters.base_filter import Filter
from filters.filter1 import Filter1
from filters.filter2 import Filter2
from filters.filter3 import Filter3
from filters.filter4 import Filter4


def run_filters() -> pd.DataFrame:
    """Execute all registered filters sequentially."""
    df = pd.DataFrame()
    filters = [subclass for subclass in Filter.__subclasses__()]
    print("Starting process...")
    start = time.time()
    for filter_cls in filters:
        df = filter_cls().apply(df)
    
    end = time.time()

    print(f"Total measured time: {end - start:.2f} seconds")
    return df


if __name__ == "__main__":
    multiprocessing.freeze_support()
    run_filters()
