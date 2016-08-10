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
This script runs a matrix workload that solves a least squares problem using
a series of matrix multiplications.
"""

import subprocess

import utils


CORES_PER_WORKER = 8
# Figure out the public hostname of the machine.
get_hostname_command = "curl -s http://169.254.169.254/latest/meta-data/public-hostname"
MASTER_HOSTNAME = subprocess.check_output(get_hostname_command, shell=True)
print "Running job with master", MASTER_HOSTNAME

workers = utils.get_workers()
total_cores = len(workers) * CORES_PER_WORKER

# Compute the parameters for the experiment.

# Increasing the number of rows increases the CPU time.
total_rows_values = [1024*1024, 2*1024*1024]
# This is basically the number of tasks; increase this for more tasks.
num_row_blocks_values = [total_cores, total_cores * 2]
# Reducing this will reduce computation by reduction^2
cols_per_block = 4096
# This is the number of times the shuffle stage will happen.
# To use less memory, reduce this number, and instead increase
# num_repeats (there's one RDD stored in memory for each shuffle
# block).
num_col_blocks = 5
# The number of times to repeat the whole computation.
num_repeats = 1

for total_rows in total_rows_values:
  for num_row_blocks in num_row_blocks_values:
    rows_per_block = int(total_rows / num_row_blocks)
    parameters = [rows_per_block, num_row_blocks, cols_per_block, num_col_blocks, num_repeats]
    stringified_parameters = [str(p) for p in parameters]
    master_url = "spark://{}:7077".format(MASTER_HOSTNAME)
    command = ("/root/spark/bin/spark-submit --class " +
      "edu.berkeley.cs.amplab.mlmatrix.BlockCoordinateDescent --driver-memory 20G " +
      "--driver-class-path /root/ml-matrix/target/scala-2.10/mlmatrix-assembly-0.1.jar " +
      "/root/ml-matrix/target/scala-2.10/mlmatrix-assembly-0.1.jar " +
      "{} {}".format(master_url, " ".join(stringified_parameters)))
    print "Running job with command: " + command
    subprocess.check_call(command, shell=True)

    # Copy the logs back.
    utils.copy_and_zip_all_logs(stringified_parameters, workers)

