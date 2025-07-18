import os
import pyarrow.parquet as pq
import pandas as pd

def inspect_parquet_dir(parquet_dir, n=5):
    for root, dirs, files in os.walk(parquet_dir):
        for fname in files:
            if fname.endswith('.parquet'):
                fpath = os.path.join(root, fname)
                print(f"\n--- {fpath} ---")
                try:
                    table = pq.read_table(fpath)
                    df = table.to_pandas()
                    print(df.head(n))
                    print(df.columns)
                    print(len(df))
                    print(df.dtypes)
                except Exception as e:
                    print(f"Failed to read {fpath}: {e}")

# Example usage:
# inspect_parquet_dir("example_output")