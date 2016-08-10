"""
This script runs a pair of two jobs: one that reads data from disk, and
a second job that uses the CPU for computation.
"""

import subprocess

import utils

slaves = [slave_line.strip("\n") for slave_line in open("/root/spark/conf/slaves").readlines()]
num_machines = len(slaves)
print "Running experiment with {} slaves: {}".format(num_machines, slaves)

cores_per_machine = 8
num_disk_tasks = num_machines * 8
items_per_partition = 10000
# Keep this large so the job stays large, and just increase the number of items per partition.
values_per_item = 10000

# Parameters for the compute-intensive job.
target_seconds = 40
available_cores = num_machines * cores_per_machine
num_compute_tasks = 8

num_concurrent_task_values = [8] #, 4, 2, 1]
for num_concurrent_tasks in num_concurrent_task_values:
  # Change the number of concurrent tasks by re-setting the Spark config.
  change_cores_command = ("sed -i s/SPARK_WORKER_CORES=.*/SPARK_WORKER_CORES=" +
                          "{}/ spark/conf/spark-env.sh".format(num_concurrent_tasks))
  print "Changing the number of Spark cores using command ", change_cores_command
  subprocess.check_call(change_cores_command, shell=True)

  copy_config_command = "/root/spark-ec2/copy-dir --delete /root/spark/conf/"
  print "Copying the new configuration to the cluster with command ", copy_config_command
  subprocess.check_call(copy_config_command, shell=True)

  # Need to stop and re-start Spark, so that the new number of cores per worker takes effect.
  subprocess.check_call("/root/spark/sbin/stop-all.sh")
  subprocess.check_call("/root/spark/sbin/start-all.sh")

  parameters = [
    num_disk_tasks,
    items_per_partition,
    values_per_item,
    target_seconds,
    available_cores,
    num_compute_tasks]

  stringified_parameters = ["{}".format(p) for p in parameters]
  command = ("/root/spark/bin/run-example DiskAndComputeJobs " +
             " ".join(stringified_parameters))
  print command
  subprocess.check_call(command, shell=True)

  utils.copy_and_zip_all_logs(parameters, slaves)

  # Clear the buffer cache, to sidestep issue with machines dying.
  subprocess.check_call("/root/ephemeral-hdfs/bin/slaves.sh /root/spark-ec2/clear-cache.sh", shell=True)
