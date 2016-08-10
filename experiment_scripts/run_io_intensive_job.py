"""
This script runs repeated trials of a job that sorts data consisting of key-value pairs. The values
are large relative to the size of the keys, and fast to serialize; as a result, the job is I/O
bound.
"""

import subprocess

import utils

target_total_data_gb = 10
# HDFS blocks are actually 128MB; round down here so that none of the output monotasks
# end up writing data to two different blocks, which we don't handle correctly.
hdfs_blocks_per_gb = 1024 / 105

slaves = utils.get_workers()
print "Running experiment assuming slaves %s" % slaves

num_machines = len(slaves)
cores_per_worker = 8
# Use a large number of values per key, so that the job spents little time computing relative
# to the amount of time doing I/O.
values_per_key = 100
num_tasks = target_total_data_gb * hdfs_blocks_per_gb
# Just do one trial for now! When experiment is properly configured, do many trials.
num_shuffles = 3

utils.cleanup_sort_job()

total_num_items = target_total_data_gb / (4.9 + values_per_key * 1.92) * (64 * 4000000)
items_per_task =  int(total_num_items / num_tasks)
data_filename = "randomData_{}_{}GB_105target".format(values_per_key,
                                                      target_total_data_gb)
use_existing_data_files = utils.check_if_hdfs_file_exists(data_filename)
# The cores_per_worker parameter won't be used by the experiment; it's just included here for
# convenience in how the log files are named.
parameters = [num_tasks, num_tasks, items_per_task, values_per_key, num_shuffles,
              data_filename, use_existing_data_files, cores_per_worker]
stringified_parameters = [str(p) for p in parameters]
command = utils.get_full_path("spark/bin/run-example")
args = "monotasks.SortJob {}".format(" ".join(stringified_parameters))
command_with_args = "{} {}".format(command, args)
print command_with_args
subprocess.check_call(command_with_args, shell=True)

utils.copy_and_zip_all_logs(stringified_parameters, slaves)

utils.cleanup_sort_job()
