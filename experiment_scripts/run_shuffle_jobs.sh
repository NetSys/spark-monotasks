"""
This script runs repeated jobs that read shuffled data.
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

num_machines = 1 
cores_per_machine = 8
items_per_partition_values = [8000000]
base_num_map_tasks = num_machines * cores_per_machine
base_num_reduce_tasks = num_machines * cores_per_machine
num_tasks_multiplier_values = [1, 2, 4, 8, 16, 32]
longs_per_value = 6
num_shuffles = 6 
sortByKey = False
cacheRdd = False
slaves = [slave_line.strip("\n") for slave_line in open("/root/spark/conf/slaves").readlines()]
print "Running experiment assuming slaves %s" % slaves

for items_per_partition in items_per_partition_values:
  for num_tasks_multiplier in num_tasks_multiplier_values:
    num_reduce_tasks = num_tasks_multiplier * base_num_reduce_tasks
    num_map_tasks = num_tasks_multiplier * base_num_map_tasks
    print "*************Running experiment with %s shuffle values" % items_per_partition
    parameters = [num_map_tasks, num_reduce_tasks, items_per_partition, longs_per_value, num_shuffles, sortByKey, cacheRdd]
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
