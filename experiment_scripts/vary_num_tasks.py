"""
This script runs jobs that process the same amount of data, but use different
numbers of tasks to do so.

Each job sorts data read from HDFS, and saves the output in HDFS. To vary the
number of tasks, we re-write the input data for each experiment, using a
different number of tasks to write the data (each task writes at least one
partition, so using more tasks forces HDFS to use more partitions).

Before running this experiment, be sure to change the block size in
ephemeral-hdfs/conf/hdfs-site.xml to be 2GB (which is the maximum allowed block size,
and larger than the largest block size used in this experiment).
"""

import os
import subprocess
import time

import utils

target_total_data_gb = 80

MEGABYTES_PER_GIGABYTE = 1024

# Smallest HDFS block size to use. We compute the number of items in each file to target this
# block size, but the file may end up being a bit larger than this. This number was chosen to
# be roughly the default HDFS block size of 128MB, with some wiggle room for the data to take
# up a bit more space.
hdfs_block_size_base = 105
hdfs_block_size_multiplier_values = [1, 2, 4, 8, 16]

slaves = [slave_line.strip("\n") for slave_line in open("/root/spark/conf/slaves").readlines()]
print "Running experiment assuming slaves %s" % slaves

num_machines = len(slaves)
values_per_key = 10
num_shuffles = 3
use_existing_data_files = False

for hdfs_block_size_multiplier in hdfs_block_size_multiplier_values:
  hdfs_block_size = hdfs_block_size_multiplier * hdfs_block_size_base
  hdfs_blocks_per_gb = MEGABYTES_PER_GIGABYTE / hdfs_block_size
  num_tasks = int(target_total_data_gb * hdfs_blocks_per_gb)

  total_num_items = target_total_data_gb / (4.9 + values_per_key * 1.92) * (64 * 4000000)
  items_per_task =  int(total_num_items / num_tasks)
  data_filename = ("randomData_%s_%sMB_blocks_%sGB_target" %
    (values_per_key, hdfs_block_size, target_total_data_gb))
  parameters = [num_tasks, num_tasks, items_per_task, values_per_key, num_shuffles,
    data_filename, use_existing_data_files]
  stringified_parameters = ["%s" % p for p in parameters]

  # Clear the buffer cache, to sidestep issue with machines dying because they've run out of memory.
  subprocess.check_call("/root/ephemeral-hdfs/sbin/slaves.sh /root/spark-ec2/clear-cache.sh",
    shell=True)

  # Delete any existing sorted data, to avoid naming conflicts.
  if num_shuffles > 0:
    try:
      subprocess.check_call("/root/ephemeral-hdfs/bin/hadoop dfs -rm -r ./*sorted*", shell=True) 
    except:
      print "No sorted data deleted, likely because no sorted data existed"

  # Run the job.
  command = "/root/spark/bin/run-example SortJob %s" % " ".join(stringified_parameters)
  print "Running sort job with command: ", command
  subprocess.check_call(command, shell=True)

  # Create a directory to store all of the logs from this experiment. Name the directory with the
  # logs based on the parameters, along with a timestamp.
  log_directory_name = "/mnt/experiment_log_%s_%s" % ("_".join(stringified_parameters), time.time())
  os.makedirs(log_directory_name)

  # Copy back all of the continuous monitor files from the slaves.
  for slave_hostname in slaves:
    continuous_monitor_relative_filename = utils.ssh_get_stdout(
      slave_hostname,
      "ls -t /tmp/ | grep continuous_monitor | head -n 1").strip("\n").strip("\r")
    continuous_monitor_filename = "/tmp/%s" % continuous_monitor_relative_filename
    local_continuous_monitor_file = "%s/%s_executor_monitor" % (log_directory_name, slave_hostname)
    print ("Copying continuous monitor from file %s on host %s back to %s" %
      (continuous_monitor_filename, slave_hostname, local_continuous_monitor_file))
    utils.scp_from(slave_hostname, continuous_monitor_filename, local_continuous_monitor_file)

  # Move the event log into the output directory with all of the logs (we move it rather than
  # copying it because we put it on a disk rather than on the root filesystem, which helps to avoid
  # running out of space on the relatively small root filesystem).
  event_log_relative_filename = subprocess.Popen(
    "ls -t /tmp/spark-events | head -n 1", stdout=subprocess.PIPE, shell=True).communicate()[0]
  print "Relative filename: ", event_log_relative_filename
  event_log_filename = "/tmp/spark-events/%s" % event_log_relative_filename.strip("\n").strip("\r")
  print "Event log filename", event_log_filename
  command = "mv %s %s/event_log" % (event_log_filename, log_directory_name)
  subprocess.check_call(command, shell=True)

  # Copy the configuration into the directory to make it easy to see later what config options were
  # used.
  subprocess.check_call("cp /root/spark/conf/spark-defaults.conf %s/" % log_directory_name,
    shell=True)
  print "Finished copying results to %s" % log_directory_name

  # Tar and zip the file so that it can easily be copied out of the cluster.
  tar_filename = log_directory_name + ".tar.gz"
  # For some reason, the tar command fails without this.
  subprocess.check_call("touch %s" % tar_filename, shell=True)

  subprocess.check_call("tar czfv %s %s/*" % (tar_filename, log_directory_name), shell=True)


