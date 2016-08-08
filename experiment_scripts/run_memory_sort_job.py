"""
This script runs jobs that process the same amount of data, but use different
numbers of tasks to do so.

Each job reads data from in-memory, sorts the data (saving intermediate shuffle
data in-memory) and stores the out in-memory.
"""

import os
import subprocess
import time

import utils

MEGABYTES_PER_GIGABYTE = 1024

slaves = utils.get_workers()
print "Running experiment assuming slaves %s" % slaves

num_machines = len(slaves)
values_per_key = 8
num_shuffles = 5

base_num_tasks = num_machines * 8
num_tasks_multipliers = [8, 4]
target_total_data_gb = num_machines * 0.5

for num_tasks_multiplier in num_tasks_multipliers:
  num_tasks = base_num_tasks * num_tasks_multiplier

  total_num_items = target_total_data_gb / (4.9 + values_per_key * 1.92) * (64 * 4000000)
  items_per_task =  int(total_num_items / num_tasks)
  parameters = [num_tasks, num_tasks, items_per_task, values_per_key, num_shuffles]
  stringified_parameters = ["%s" % p for p in parameters]

  # Clear the buffer cache, to sidestep issue with machines dying because they've run out of memory.
  slaves_file = utils.get_full_path("spark/sbin/slaves.sh")
  clear_cache_file = utils.get_full_path("spark-ec2/clear-cache.sh")
  subprocess.check_call("{} {}".format(slaves_file, clear_cache_file), shell=True)

  # Delete any existing sorted data, to avoid naming conflicts.
  if num_shuffles > 0:
    try:
      hadoop_command = utils.get_full_path("ephemeral-hdfs/bin/hadoop")
      subprocess.check_call("{} dfs -rm -r ./*sorted*".format(hadoop_command), shell=True)
    except:
      print "No sorted data deleted, likely because no sorted data existed"

  # Run the job.
  run_example_command = utils.get_full_path("spark/bin/run-example")
  command = "{} monotasks.MemorySortJob {}".format(
    run_example_command, " ".join(stringified_parameters))
  print "Running sort job with command: ", command
  subprocess.check_call(command, shell=True)

  utils.copy_and_zip_all_logs(stringified_parameters, slaves)

