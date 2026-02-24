import pandas as pd
import numpy as np

# 1. Create Dummy Data
# 5,000 products
products = pd.DataFrame({
    'product_id': range(25000),
    'base_price': np.random.uniform(10, 100, 25000)
})

# 5,000 stores
stores = pd.DataFrame({
    'store_id': range(25000),
    'tax_rate': np.random.uniform(1.05, 1.25, 25000)
})

print("Pandas: Attempting Cross Join (Cartesian Product)...")

try:
    # --- THE MEMORY KILLER ---
    # Pandas materializes 25,000,000 rows in memory immediately.
    # Each row has floats and ints. The overhead of the Index and Python objects 
    # spikes memory usage massively (easily 2GB+ just for this simple frame).
    merged = products.merge(stores, how='cross')
    
    # Calculate final price
    merged['final_price'] = merged['base_price'] * merged['tax_rate']
    
    # Aggregate
    result = merged.groupby('store_id')['final_price'].mean()
    print("Pandas: Success! (Unlikely)")
    
except MemoryError:
    print("\n❌ CRITICAL: Memory Error! Pandas could not hold the intermediate table.")
except Exception as e:
    print(f"\n❌ CRITICAL: System killed the process or froze: {e}")