import time
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc

start_time = time.perf_counter()
# replace the path with your actual CSV file location
csv_path = r"data/measurements_100_mill.txt"
partial_results = []

my_labels = ["station", "temperature"]
read_options = csv.ReadOptions(column_names=my_labels)
parse_options = csv.ParseOptions(delimiter=";")


with csv.open_csv(
    csv_path, read_options=read_options, parse_options=parse_options
) as reader:
    for chunk in reader:
        table = pa.Table.from_batches([chunk])
        stats = table.group_by("station").aggregate(
            [
                ("temperature", "min"),
                ("temperature", "max"),
                ("temperature", "sum"),
                ("temperature", "count"),
            ]
        )
        partial_results.append(stats)

final_stats = (
    pa.concat_tables(partial_results)
    .group_by("station")
    .aggregate(
        [
            ("temperature_min", "min"),
            ("temperature_sum", "sum"),
            ("temperature_count", "sum"),
            ("temperature_max", "max"),
        ]
    )
)


final_stats = final_stats.append_column(
    "temperature_mean",
    pc.divide(final_stats["temperature_sum_sum"], final_stats["temperature_count_sum"]),
)

final_stats = final_stats.select(
    ["station", "temperature_min_min", "temperature_mean", "temperature_max_max"]
)

end_time = time.perf_counter()
print(final_stats)
print(f"Time taken: {end_time - start_time:.2f} seconds")
