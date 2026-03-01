import time
from itertools import islice
import concurrent.futures

start_time = time.perf_counter()


ARRAY_BUFFER_LINES = 100_000
partition_futures = []


def process_slice(slice):
    dict_stations = {}
    for line in slice:
        station, temp_str = line.split(";")
        try:
            temp = float(temp_str)
        except ValueError:
            continue  # skip lines with invalid temperature

        if station not in dict_stations:
            dict_stations[station] = [temp, temp, temp, 1]  # min, max, sum, count
        else:
            min_temp, max_temp, sum_temp, count = dict_stations[station]
            if temp < min_temp:
                dict_stations[station][0] = temp  # update min
            elif temp > max_temp:
                dict_stations[station][1] = temp  # update max
            dict_stations[station][2] += temp  # update sum
            dict_stations[station][3] += 1  # update count

    return dict_stations


with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    with open("data/measurements.txt", "r", encoding="utf-8") as f:
        while True:
            next_slice = list(islice(f, ARRAY_BUFFER_LINES))

            if not next_slice:
                break
            partition_futures.append(executor.submit(process_slice, next_slice))

    partition_dicts = [
        future.result() for future in concurrent.futures.as_completed(partition_futures)
    ]

final_dict = {}
for partition_dict in partition_dicts:
    for station, (min_temp, max_temp, sum_temp, count) in partition_dict.items():
        if station not in final_dict:
            final_dict[station] = [min_temp, max_temp, sum_temp, count]
        else:
            final_min, final_max, final_sum, final_count = final_dict[station]
            if min_temp < final_min:
                final_dict[station][0] = min_temp  # update min
            if max_temp > final_max:
                final_dict[station][1] = max_temp  # update max
            final_dict[station][2] += sum_temp  # update sum
            final_dict[station][3] += count  # update count


for station, (min_temp, max_temp, sum_temp, count) in final_dict.items():
    final_dict[station] = [min_temp, max_temp, sum_temp, count, sum_temp / count]

end_time = time.perf_counter()

for station, (min_temp, max_temp, sum_temp, count, mean_temp) in final_dict.items():
    mean_temp = sum_temp / count
    print(f"{station}: min={min_temp}, mean={mean_temp}, max={max_temp}")


print(f"Time taken: {end_time - start_time:.2f} seconds")
