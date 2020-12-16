import subprocess
import os

TOTAL_ROWS = 12748986

name = "mpi_weak_scale.csv"
try:
    os.remove(name)
except OSError:
    pass

for i in range(1,6):
    num_cpus = 2**(i-1)
    num_rows = i
    # num_rows = 5
    subprocess.run(["python3", "mpi_benchmark.py", name, str(num_cpus), str(num_rows)])
    print("hi")

print(name, str(num_cpus), str(num_rows))
