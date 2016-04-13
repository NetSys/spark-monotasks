"""
This script runs repeated jobs that each read the same amount of data, using
different numbers of tasks.
"""

import subprocess

import utils

megabyte_in_bytes = 1024 * 1024
gigabyte_in_bytes = 1024 * megabyte_in_bytes
target_total_data_bytes = gigabyte_in_bytes * 128

# Approximate number of items per byte, when using 2 longs per value.
items_per_byte = 0.02936

# Compute total number of items, then per-task value!
total_items = target_total_data_bytes * items_per_byte


num_machines = 1
cores_per_machine = 8
num_tasks_multipliers = [128, 64, 32]
longs_per_value = 2
num_shuffles = 1
sortByKey = False
cacheRdd = False
slaves = [slave_line.strip("\n") for slave_line in open("/root/spark/conf/slaves").readlines()]
print "Running experiment assuming slaves {}".format(slaves)

for num_tasks_multiplier in num_tasks_multipliers:
  num_tasks = num_machines * cores_per_machine * num_tasks_multiplier
  items_per_task = int(total_items / num_tasks)
  parameters = [num_tasks, num_tasks, items_per_task, longs_per_value, num_shuffles, sortByKey, cacheRdd]
  stringified_parameters = ["{}".format(p) for p in parameters]
  command = "/root/spark/bin/run-example monotasks.ShuffleJob {}".format(
    " ".join(stringified_parameters))
  print command
  subprocess.check_call(command, shell=True)

  utils.copy_and_zip_all_logs(stringified_parameters, slaves)

  # Clear the buffer cache, to sidestep issue with machines dying.
  subprocess.check_call("/root/ephemeral-hdfs/bin/slaves.sh /root/spark-ec2/clear-cache.sh", shell=True)
