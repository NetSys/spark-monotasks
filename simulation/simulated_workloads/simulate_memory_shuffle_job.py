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
"""Simulates an in-memory shuffle job.

Simulates a 2-worker cluster, where each worker has 8 CPU cores and no disks. The network bandwidth
is 1Gbps and the network latency is 1ms.

Run this script using the command "python -m simulated_workloads.simulate_memory_shuffle_job -h"
from the "simulation" directory.
"""

import argparse
import logging
import random

from simulation import simulation_conf
from simulation import simulator
from simulation import task_constructs

NUM_WORKERS = 2
NUM_CORES_PER_WORKER = 8
# 1 Gb/s = 125000 B/ms
NETWORK_BANDWIDTH_BPMS = 125000


def main():
  args = __parse_args()
  logging.basicConfig(level=args.log_level)

  # Initialize the "random" module's seed value to 0 so that multiple runs of the Simulator use the
  # same pseudo-random numbers.
  random.seed(0)

  compute_monotask_time_ms = 1000
  ideal_total_reduce_phase_compute_time_ms = ((args.num_partitions * compute_monotask_time_ms) /
    NUM_CORES_PER_WORKER)
  ideal_total_reduce_phase_network_time_ms = (ideal_total_reduce_phase_compute_time_ms *
    args.network_to_compute_ratio)
  total_shuffle_size_bytes = (NETWORK_BANDWIDTH_BPMS * NUM_WORKERS *
    ideal_total_reduce_phase_network_time_ms)
  logging.info(
    "Using a compute time of %s ms and a total shuffle data size of %s bytes.",
    compute_monotask_time_ms,
    total_shuffle_size_bytes)

  build_conf_and_simulate(
    args.continuous_monitor_dir,
    args.scheduling_mode,
    args.num_partitions,
    compute_monotask_time_ms,
    args.reduce_stage_compute_variance,
    total_shuffle_size_bytes,
    args.network_variance)


def __parse_args():
  """ Parses and validates the command line arguments. """
  parser = argparse.ArgumentParser(
    description="Generates a graph of variance vs. Job completion time.")
  parser.add_argument(
    "-l",
    "--log-level",
    choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    default="WARNING",
    help="The verbosity of standard logging. See the \"logging\" package for more information.")
  parser.add_argument(
    "-o",
    "--continuous-monitor-dir",
    help="The directory that the continuous monitor logs should be written to.",
    required=True)
  parser.add_argument(
    "-m",
    "--scheduling-mode",
    choices=["even-distribution", "fixed-slots", "throttling"],
    default="fixed-slots",
    help="The macrotask scheduling mode to use.")
  parser.add_argument(
    "-p",
    "--num-partitions",
    help="The number of partitions in the map phase and the reduce phase.",
    required=True,
    type=int)
  parser.add_argument(
    "-r",
    "--network-to-compute-ratio",
    default=1.0,
    help="The ratio of network time to compute time in the reduce phase.",
    type=float)
  parser.add_argument(
    "-c",
    "--compute-variance",
    default=0.0,
    dest="reduce_stage_compute_variance",
    help="The amount of random variance to add to compute monotasks in the reduce stage.",
    type=float)
  parser.add_argument(
    "-n",
    "--network-variance",
    default=0.0,
    help=("The amount of random variance to add to the network bandwidth during each network " +
      "request."),
    type=float)

  args = parser.parse_args()
  check_is_positive(args.num_partitions, description="number of partitions")
  check_variance(args.reduce_stage_compute_variance, variance_type="reduce stage compute")
  check_variance(args.network_variance, variance_type="network")
  return args


def check_is_positive(value, description, units=""):
  """ Verifies that the provided value is positive. """
  assert value > 0, "The %s must be positive, but is: %s %s" % (description, value, units)


def check_variance(variance, variance_type):
  """ Verifies that the provided variance value is in the range [0, 1). """
  assert (variance >= 0) and (variance < 1), \
    "The %s variance (%s) must be in the range [0, 1)." % (variance_type, variance)


def build_conf_and_simulate(
    continuous_monitor_dir,
    scheduling_mode,
    num_partitions,
    compute_time_ms,
    reduce_stage_compute_variance,
    total_shuffle_size_bytes,
    network_variance):
  """Builds a configuration that specifies an in-memory shuffle Job and simulates it.

  Creates and simulates a two-Stage Job where the first Stage does only compute and the second Stage
  reads shuffle data and then computes on it. All shuffle data is assumed to be stored in memory.

  Returns:
    The finished Simulator.
  """
  conf = simulation_conf.SimulationConf()
  conf.num_workers = NUM_WORKERS
  conf.scheduling_mode = scheduling_mode
  conf.throttling_scheduler_macrotask_buffer_size = 6
  conf.num_cores = NUM_CORES_PER_WORKER
  conf.network_bandwidth_Bpms = NETWORK_BANDWIDTH_BPMS
  conf.network_bandwidth_variance = network_variance
  conf.network_latency_ms = 1.0
  conf.jobs = [__create_job(
    num_partitions, compute_time_ms, reduce_stage_compute_variance, total_shuffle_size_bytes)]

  return simulator.simulate(
    continuous_monitor_dir, continuous_monitor_interval_ms=10.0, conf=conf)


def __create_job(
    num_partitions,
    compute_time_ms,
    reduce_stage_compute_variance,
    total_shuffle_size_bytes):
  """ Returns an in-memory shuffle Job configured with the provided parameters. """
  job = task_constructs.Job()
  map_stage = task_constructs.Stage(job)
  for _ in xrange(num_partitions):
    __create_macrotask_for_stage(
      map_stage,
      num_partitions,
      compute_time_ms,
      compute_variance=0.0,
      shuffle_bytes_per_macrotask=0.0)

  reduce_stage = task_constructs.Stage(job)
  shuffle_bytes_per_macrotask = total_shuffle_size_bytes / num_partitions
  for _ in xrange(num_partitions):
    __create_macrotask_for_stage(
      reduce_stage,
      num_partitions,
      compute_time_ms,
      reduce_stage_compute_variance,
      shuffle_bytes_per_macrotask)
  return job


def __create_macrotask_for_stage(
    stage,
    num_partitions,
    compute_time_ms,
    compute_variance,
    shuffle_bytes_per_macrotask):
  """ Adds a Macrotask to the provided Stage, configured with the provided parameters. """
  task_constructs.ComputeMonotask(
    task_constructs.Macrotask(stage),
    simulation_conf.SimulationConf.get_compute_time_ms(compute_time_ms, compute_variance),
    shuffle_bytes_per_macrotask,
    is_shuffle_data_on_disk=False,
    num_partitions_in_current_stage=num_partitions)


if __name__ == "__main__":
  main()
