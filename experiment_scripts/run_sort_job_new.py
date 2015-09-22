"""
This script runs repeated jobs that each sort the same amount of data, using
different numbers of values for each key.
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

target_total_data_gb = 200
# HDFS blocks are actually 128MB; round down here so that none of the output monotasks
# end up writing data to two different blocks, which we don't handle correctly.
hdfs_blocks_per_gb = 1024 / 105

slaves = [slave_line.strip("\n") for slave_line in open("/root/spark/conf/slaves").readlines()]
print "Running experiment assuming slaves %s" % slaves

num_machines = len(slaves)
values_per_key_values = [10, 25, 100, 1]
num_tasks = target_total_data_gb * hdfs_blocks_per_gb
# Just do one trial for now! When experiment is properly configured, do many trials.
num_shuffles = 3
cores_per_worker_values = [8, 4]
use_existing_data_files = False

for cores_per_worker in cores_per_worker_values:
  # Change the number of concurrent tasks by re-setting the Spark config.
  change_cores_command = ("sed -i s/SPARK_WORKER_CORES=.*/SPARK_WORKER_CORES=" +
    "%s/ /root/spark/conf/spark-env.sh" % cores_per_worker)
  print "Changing the number of Spark cores using command ", change_cores_command
  subprocess.check_call(change_cores_command, shell=True)

  copy_config_command = "/root/spark-ec2/copy-dir --delete /root/spark/conf/"
  print "Copying the new configuration to the cluster with command ", copy_config_command
  subprocess.check_call(copy_config_command, shell=True)

  # Need to stop and re-start Spark, so that the new number of cores per worker takes effect.
  subprocess.check_call("/root/spark/sbin/stop-all.sh")
  subprocess.check_call("/root/spark/sbin/start-all.sh")

  for values_per_key in values_per_key_values:
    total_num_items = target_total_data_gb / (4.9 + values_per_key * 1.92) * (64 * 4000000)
    items_per_task =  int(total_num_items / num_tasks)
    data_filename = "randomData_%s_%sGB_105target" % (values_per_key, target_total_data_gb)
    # The cores_per_worker parameter won't be used by the experiment; it's just included here for
    # convenience in how the log files are named.
    parameters = [num_tasks, num_tasks, items_per_task, values_per_key, num_shuffles,
      data_filename, use_existing_data_files, cores_per_worker]
    stringified_parameters = ["%s" % p for p in parameters]
    command = "/root/spark/bin/run-example SortJob %s" % " ".join(stringified_parameters)
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
    subprocess.check_call("/root/ephemeral-hdfs/sbin/slaves.sh /root/spark-ec2/clear-cache.sh", shell=True)

    # Delete any sorted data.
    subprocess.check_call("/root/ephemeral-hdfs/bin/hadoop dfs -rm -r ./*sorted*", shell=True)

  # Future numbers of cores_per_worker don't need to re-generate the data files, and can instead just use the existing ones.
  use_existing_data_files = True
