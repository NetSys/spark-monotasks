"""
This script runs jobs that process the same amount of data, but use different
numbers of tasks to do so.

Each job consists of one stage that reads data from HDFS and computes on it.
"""

import subprocess

import utils


workers = utils.get_workers()
print "Running experiment assuming slaves {}".format(workers)

num_workers = len(workers)
items_per_value = 13
num_trials = 6
cpu_iterations_per_item = 25

base_num_tasks = num_workers * 8
num_tasks_multipliers = [1, 2, 4, 8, 16]
target_total_data_gb = 40

for num_tasks_multiplier in num_tasks_multipliers:
  num_tasks = base_num_tasks * num_tasks_multiplier

  total_num_items = target_total_data_gb / (4.9 + items_per_value * 1.92) * (64 * 4000000)
  items_per_task = int(total_num_items / num_tasks)
  parameters = [
    num_workers,
    num_tasks,
    items_per_task,
    items_per_value,
    cpu_iterations_per_item,
    num_trials
  ]
  stringified_parameters = ["{}".format(p) for p in parameters]

  # Run the job. The job clears the buffer cache before each trial, so this script does not need to.
  disk_cpu_job_path = utils.get_full_path("spark/bin/run-example monotasks.DiskCpuJob")
  command = "{} {}".format(disk_cpu_job_path, " ".join(stringified_parameters))
  print "Running job with command: ", command
  subprocess.check_call(command, shell=True)

  utils.copy_and_zip_all_logs(stringified_parameters, workers)
