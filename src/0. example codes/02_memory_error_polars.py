import polars as pl
import numpy as np

# 1. Create Dummy Data (Polars is faster at creation too)
products = pl.DataFrame({
    'product_id': range(25000),
    'base_price': np.random.uniform(10, 100, 25000)
})

stores = pl.DataFrame({
    'store_id': range(25000),
    'tax_rate': np.random.uniform(1.05, 1.25, 25000)
})

print("Polars: Attempting Cross Join with Lazy Streaming...")

# 2. Lazy Execution
# Convert to LazyFrame. No data is processed yet.
q = (
    products.lazy()
    .join(stores.lazy(), how="cross")  # The "Exploding" Join
    .with_columns(
        (pl.col("base_price") * pl.col("tax_rate")).alias("final_price")
    )
    .group_by("store_id")
    .agg(pl.col("final_price").mean())
)

# 3. Collect with Streaming
# streaming=True tells Polars to process in chunks.
# It joins a few rows -> calculates -> discards -> repeats.
# RAM usage stays low, even if the intermediate join is billions of rows.
result = q.collect(streaming=True)

print(f"✅ Polars Success! Result shape: {result.shape}")
print(result.head())