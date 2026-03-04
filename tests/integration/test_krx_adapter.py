import os
from dotenv import load_dotenv

def run_adapter_test():
    load_dotenv()
    
    # Import the actual adapter implementation we just fixed
    from src.infrastructure.krx_adapter import KrxDirectStockInfoAdapter
    
    print("=== Initializing KrxDirectStockInfoAdapter ===")
    
    # Initialization will automatically run _login
    adapter = KrxDirectStockInfoAdapter()
    
    import datetime
    target_date = datetime.date(2026, 3, 3)
    
    print("\n=== 1. fetch_today_ceiling_stocks ===")
    ceilings = adapter.fetch_today_ceiling_stocks(target_date)
    print(f"Found {len(ceilings)} ceiling stocks.")
    if ceilings:
        print("First 2:", ceilings[:2])
    
    print("\n=== 2. fetch_current_prices ===")
    prices = adapter.fetch_current_prices(['005930', 'SK하이닉스'], target_date)
    print("Prices:", prices)
    
    print("\n=== 3. fetch_ohlcv_bulk ===")
    start_date = datetime.date(2026, 2, 20)
    bulk_data = adapter.fetch_ohlcv_bulk(['005930'], start_date, target_date)
    print(f"Fetched bulk data for {list(bulk_data.keys())}")
    if '005930' in bulk_data:
        df = bulk_data['005930']
        print(f"Shape: {df.shape}")
        print(df.tail(2))
        
    print("\n=== 4. fetch_candidates_in_range ===")
    candidates = adapter.fetch_candidates_in_range(start_date, target_date)
    print(f"Candidates found for {len(candidates)} days.")
    for d, c in list(candidates.items())[:2]:
        print(f"{d}: {len(c)} stocks")
        
if __name__ == "__main__":
    run_adapter_test()
