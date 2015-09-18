"""
This script runs a pair of two jobs: one that reads data from disk, and
a second job that uses the CPU for computation.
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

slaves = [slave_line.strip("\n") for slave_line in open("/root/spark/conf/slaves").readlines()]
num_machines = len(slaves)
print "Running experiment with %s slaves: %s" % (num_machines, slaves)
 
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
    "%s/ spark/conf/spark-env.sh" % num_concurrent_tasks)
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

  stringified_parameters = ["%s" % p for p in parameters]
  command = "/root/spark/bin/run-example DiskAndComputeJobs %s" % " ".join(stringified_parameters)
  print command

  subprocess.check_call(command, shell=True)
  # Name the directory with the logs based on the parameters, along with a timestamp.
  log_directory_name = "/mnt/disk_and_compute_log_%s_%s" % ("_".join(stringified_parameters), time.time())
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
