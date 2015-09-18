"""
This script runs repeated jobs that each read the same amount of data, using
different numbers of tasks.
"""

import os
import subprocess
import time

# Copy a file from a given host through scp, throwing an exception if scp fails.
def scp_from(host, remote_file, local_file):
  subprocess.check_call(
    "scp -q -o StrictHostKeyChecking=no '%s:%s' '%s'" %
    (host, remote_file, local_file), shell=True)

def ssh_get_stdout(host, command):
  command = "source /root/.bash_profile; %s" % command
  ssh_command = ("ssh -t -o StrictHostKeyChecking=no %s '%s'" %
    (host, command))
  return subprocess.Popen(ssh_command, stdout=subprocess.PIPE, shell=True).communicate()[0]

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
print "Running experiment assuming slaves %s" % slaves

for num_tasks_multiplier in num_tasks_multipliers:
  num_tasks = num_machines * cores_per_machine * num_tasks_multiplier
  items_per_task = int(total_items / num_tasks)
  parameters = [num_tasks, num_tasks, items_per_task, longs_per_value, num_shuffles, sortByKey, cacheRdd]
  stringified_parameters = ["%s" % p for p in parameters]
  command = "/root/spark/bin/run-example ShuffleJob %s" % " ".join(stringified_parameters)
  print command
  subprocess.check_call(command, shell=True)
  # Name the directory with the logs based on the parameters, along with a timestamp.
  log_directory_name = "/mnt/experiment_log_%s_%s" % ("_".join(stringified_parameters), time.time())
  os.makedirs(log_directory_name)

  for slave_hostname in slaves:
    continuous_monitor_relative_filename = ssh_get_stdout(
      slave_hostname,
      "ls -t /tmp/ | grep continuous_monitor | head -n 1").strip("\n").strip("\r")
    continuous_monitor_filename = "/tmp/%s" % continuous_monitor_relative_filename
    local_continuous_monitor_file = "%s/%s_executor_monitor" % (log_directory_name, slave_hostname)
    print ("Copying continuous monitor from file %s on host %s back to %s" % 
      (continuous_monitor_filename, slave_hostname, local_continuous_monitor_file))
    scp_from(slave_hostname, continuous_monitor_filename, local_continuous_monitor_file)

  event_log_relative_filename = subprocess.Popen(
    "ls -t /tmp/spark-events | head -n 1", stdout=subprocess.PIPE, shell=True).communicate()[0]
  print "Relative filename: ", event_log_relative_filename
  event_log_filename = "/tmp/spark-events/%s" % event_log_relative_filename.strip("\n").strip("\r")
  print "Event log filename", event_log_filename
  command = "mv %s %s/event_log" % (event_log_filename, log_directory_name)
  subprocess.check_call(command, shell=True)

  # Copy the configuration into the directory to make it easy to see config later.
  subprocess.check_call("cp /root/spark/conf/spark-defaults.conf %s/" % log_directory_name, shell=True)
  print "Finished copying results to %s" % log_directory_name
    
  # Tar and zip the file so that it can easily be copied out of the cluster.
  tar_filename = log_directory_name + ".tar.gz"

  # For some reason, the tar command fails without this.
  subprocess.check_call("touch %s" % tar_filename, shell=True)

  subprocess.check_call("tar czfv %s %s/*" % (tar_filename, log_directory_name), shell=True)

  # Clear the buffer cache, to sidestep issue with machines dying.
  subprocess.check_call("/root/ephemeral-hdfs/bin/slaves.sh /root/spark-ec2/clear-cache.sh", shell=True)
