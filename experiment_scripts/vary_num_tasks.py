"""
This script runs jobs that process the same amount of data, but use different
numbers of tasks to do so.

Each job sorts in-memory data.  Input, intermediate shuffle data, and output
data are all stored in-memory, so the job uses only CPU and network resources.
"""

import subprocess

import utils

slaves = utils.get_workers()
print "Running experiment assuming slaves {}".format(slaves)

num_machines = len(slaves)
values_per_key = 10
num_shuffles = 3

base_num_tasks = num_machines * 8
num_tasks_multipliers = [1, 2, 4, 8, 16]
target_total_data_gb = num_machines * 3

for num_tasks_multiplier in num_tasks_multipliers:
  num_tasks = base_num_tasks * num_tasks_multiplier

  total_num_items = target_total_data_gb / (4.9 + values_per_key * 1.92) * (64 * 4000000)
  items_per_task =  int(total_num_items / num_tasks)
  parameters = [num_tasks, num_tasks, items_per_task, values_per_key, num_shuffles]
  stringified_parameters = ["{}".format(p) for p in parameters]

  # Clear the buffer cache, to sidestep issue with machines dying because they've run out of memory.
  slaves_file = utils.get_full_path("ephemeral-hdfs/sbin/slaves.sh")
  clear_cache_file = utils.get_full_path("spark-ec2/clear-cache.sh")
  subprocess.check_call("{} {}".format(slaves_file, clear_cache_file), shell=True)

  # Run the job.
  memory_sort_job_path = utils.get_full_path("spark/bin/run-example monotasks.MemorySortJob")
  command = "{} {}".format(memory_sort_job_path,
                           " ".join(stringified_parameters))
  print "Running sort job with command: ", command
  subprocess.check_call(command, shell=True)

  utils.copy_and_zip_all_logs(stringified_parameters, slaves)
