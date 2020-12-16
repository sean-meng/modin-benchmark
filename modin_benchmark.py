import os
import csv
import sys

# os.environ["MODIN_ENGINE"] = "ray"  # Modin will use Ray
os.environ["MODIN_ENGINE"] = "dask"  # Modin will use Dask
# os.environ["MODIN_ENGINE"] = "mpi"  # Modin will use Ray

# pass in file_name, num_cpus, num_rows
if __name__ == "__main__":
    import time

    file_name = sys.argv[1]
    num_cpus = sys.argv[2]
    csv_size = sys.argv[3]

    os.environ["MODIN_CPUS"] = num_cpus

    # import ray
    # ray.init(num_cpus=int(num_cpus))
    # ray.init()

    import modin.pandas as pd

    pandas_duration_all = []

    pandas_duration_list = []
    for i in range(10):
        file_path = csv_size + ".csv"
        print(file_path)
        start = time.time()
        pandas_df = pd.read_csv(file_path, parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"], quoting=3)
        end = time.time()
        pandas_duration = end - start
        pandas_duration_list.append(pandas_duration)
    pandas_duration_all.append(pandas_duration_list)

    pandas_duration_list = []
    for i in range(10):
        start = time.time()
        pandas_count = pandas_df.count()
        end = time.time()
        pandas_duration = end - start
        pandas_duration_list.append(pandas_duration)
    pandas_duration_all.append(pandas_duration_list)

    pandas_duration_list = []
    for i in range(10):
        start = time.time()
        pandas_isnull = pandas_df.isnull()
        end = time.time()
        pandas_duration = end - start
        pandas_duration_list.append(pandas_duration)
    pandas_duration_all.append(pandas_duration_list)

    pandas_duration_list = []
    for i in range(10):
        start = time.time()
        rounded_trip_distance_pandas = pandas_df["trip_distance"].apply(round)
        end = time.time()
        pandas_duration = end - start
        pandas_duration_list.append(pandas_duration)
    pandas_duration_all.append(pandas_duration_list)

    pandas_duration_list = []
    for i in range(10):
        start = time.time()
        pandas_df["rounded_trip_distance"] = rounded_trip_distance_pandas
        end = time.time()
        pandas_duration = end - start
        pandas_duration_list.append(pandas_duration)
    pandas_duration_all.append(pandas_duration_list)

    pandas_duration_list = []
    for i in range(10):
        start = time.time()
        pandas_groupby = pandas_df.groupby(by="rounded_trip_distance").count()
        end = time.time()
        pandas_duration = end - start
        pandas_duration_list.append(pandas_duration)
    pandas_duration_all.append(pandas_duration_list)

    with open(file_name, 'a') as f:
        writer = csv.writer(f)
        writer.writerows(pandas_duration_all)
