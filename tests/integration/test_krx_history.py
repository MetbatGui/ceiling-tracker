import os
import datetime
from dotenv import load_dotenv
from src.infrastructure.krx_adapter import KrxDirectStockInfoAdapter

def test_history_length():
    load_dotenv()
    adapter = KrxDirectStockInfoAdapter()
    
    start_date = datetime.date(1990, 1, 1)
    end_date = datetime.date(2026, 3, 4)
    
    # Try fetching history for 005930 (Samsung)
    print("Fetching bulk OHLCV from 1990 to 2026...")
    bulk = adapter.fetch_ohlcv_bulk(['005930'], start_date, end_date)
    
    if '005930' in bulk:
        df = bulk['005930']
        print(f"Total rows: {len(df)}")
        print(f"Earliest date in data: {df.index.min()}")
        print(f"Latest date in data: {df.index.max()}")
        if not df.empty:
            print("First few rows:")
            print(df.head(2))
    else:
        print("No data fetched.")

if __name__ == "__main__":
    test_history_length()
