import time
import polars as pl


start_time = time.perf_counter()
# replace the path with your actual CSV file location
csv_path = r"data/measurements_100_mill.txt"

df = pl.scan_csv(csv_path, separator=";", has_header=False, new_columns=["station", "temperature"])

final_stats = df.group_by("station").agg([
    pl.col("temperature").min().alias("temperature_min"),
    pl.col("temperature").max().alias("temperature_max"),
    pl.col("temperature").mean().alias("temperature_mean")
]).collect().sort("station")

end_time = time.perf_counter()
print(final_stats)
print(f"Time taken: {end_time - start_time:.2f} seconds")
