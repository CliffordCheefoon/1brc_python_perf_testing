import pandas as pd
import time

start_time = time.perf_counter()
csv_path = r"data/measurements.txt"

df = pd.read_csv(csv_path, sep=";", engine="pyarrow", dtype_backend="pyarrow")

if df.columns.tolist() != ["station", "temperature"]:
    df.columns = ["station", "temperature"]

df["temperature"] = pd.to_numeric(df["temperature"], errors="coerce")

stats = df.groupby("station")["temperature"].agg(min="min", mean="mean", max="max")

end_time = time.perf_counter()
print(stats)
print(f"Time taken: {end_time - start_time:.2f} seconds")
