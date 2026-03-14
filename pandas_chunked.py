import pandas as pd
import time
import sys

start_time = time.perf_counter()
# replace the path with your actual CSV file location
CHUNK_SIZE = 100_000
csv_path = r"data/measurements.txt"
partial_results = []


chunks = pd.read_csv(
    csv_path,
    sep=";",
    chunksize=CHUNK_SIZE,
)

for chunk in chunks:
    chunk.columns = ["station", "temperature"]

    stats = chunk.groupby("station")["temperature"].agg(
        min="min", max="max", sum="sum", count="count"
    )
    partial_results.append(stats)

final_stats = (
    pd.concat(partial_results)
    .groupby("station")
    .agg(
        min=("min", "min"),
        sum=("sum", "sum"),
        count=(
            "count",
            "sum",
        ),  # this is the 'sum' operation over the 'count' column to get the total count for each station
        max=("max", "max"),
    )
)
final_stats["mean"] = final_stats["sum"] / final_stats["count"]
final_stats = final_stats[["min", "mean", "max"]]  # reorder columns


end_time = time.perf_counter()
#print(final_stats)
test_tag = sys.argv[1]
print(f"{test_tag} || Time taken: {end_time - start_time:.2f} seconds")
