import os
import csv
import sys

import time

import pandas

pandas_df = pandas.read_csv("5.csv", parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"], quoting=3)

TOTAL_ROWS = 12748986
pandas_df[:TOTAL_ROWS//32].to_csv(path_or_buf="0.csv")
pandas_df[:TOTAL_ROWS//64].to_csv(path_or_buf="-1.csv")
pandas_df[:TOTAL_ROWS//128].to_csv(path_or_buf="-2.csv")
pandas_df[:TOTAL_ROWS//256].to_csv(path_or_buf="-3.csv")
