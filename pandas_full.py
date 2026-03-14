import sys

import pandas as pd
import time


start_time = time.perf_counter()
csv_path = r"data/measurements_100_mill.txt"

df = pd.read_csv(csv_path, sep=";")

df.columns = ["station", "temperature"]

stats = df.groupby("station")["temperature"].agg(min="min", mean="mean", max="max")

end_time = time.perf_counter()
#print(stats)
test_tag = sys.argv[1]
print(f"{test_tag} || Time taken: {end_time - start_time:.2f} seconds")
