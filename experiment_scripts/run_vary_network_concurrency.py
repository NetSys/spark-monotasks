#
# Copyright 2016 The Regents of The University California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
This script repeatedly runs an in-memory shuffle, varying the value of the parameter
spark.monotasks.network.maxConcurrentTasks.

It assumes that spark-defaults.conf already has some value of
spark.monotasks.network.maxConcurrentTasks set.
"""

import subprocess

import utils

workers = utils.get_workers()
num_workers = len(workers)
print "Running experiment with %s workers: %s" % (num_workers, workers)

values_per_key = 6
num_shuffles = 15

# Run enough tasks to have 4 waves.
num_tasks = num_workers * 8 * 4

# Target just one GB of data per machine, to minimize the effect of garbage collection.
target_total_data_gb = num_workers * 1

max_concurrent_tasks_values = [0, 2, 4, 8, 16]

spark_defaults_filepath = utils.get_full_path(relative_path="spark/conf/spark-defaults.conf")
run_example_command = utils.get_full_path(relative_path="spark/bin/run-example")
run_on_slaves_command = utils.get_full_path(relative_path="ephemeral-hdfs/sbin/slaves.sh")

for max_concurrent_tasks in max_concurrent_tasks_values:
  # Change the maximum number of concurrent tasks by resetting the Spark config.
  change_max_concurrent_tasks_command = ("sed -i \"s/maxConcurrentTasks .*/" +
    "maxConcurrentTasks {}/\" {}".format(max_concurrent_tasks, spark_defaults_filepath))
  print "Changing the maximum number of concurrent tasks using command: {}".format(
    change_max_concurrent_tasks_command)
  subprocess.check_call(change_max_concurrent_tasks_command, shell=True)

  total_num_items = target_total_data_gb / (4.9 + values_per_key * 1.92) * (64 * 4000000)
  items_per_task =  int(total_num_items / num_tasks)
  parameters = [
    num_tasks,
    num_tasks,
    items_per_task,
    values_per_key,
    num_shuffles]
  stringified_parameters = [str(p) for p in parameters]
  experiment_command = ("{} monotasks.MemorySortJob {}".format(
    run_example_command, " ".join(stringified_parameters)))
  print "Running experiment using command: %s" % experiment_command
  subprocess.check_call(experiment_command, shell=True)

  utils.copy_and_zip_all_logs(stringified_parameters + [str(max_concurrent_tasks)], workers)
