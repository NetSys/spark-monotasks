#
# Copyright 2015 The Regents of The University California
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
"""Contains integration tests of the Monotasks Simulator.

Run the tests in this file by executing the command "py.test integration_tests.py" from the
containing directoy.
"""

import copy
from datetime import datetime
import logging
import os
from os import path
import pytest
import shutil

from simulation import simulation_conf
from simulation import simulator
from simulation import task_constructs

# Set the logging level to INFO so that logs are printed if a test fails.
logging.basicConfig(level=logging.INFO)

CONTINUOUS_MONITOR_PREFIX = "continuous_monitor_logs"


@pytest.fixture(scope="function")
def cleanup(request):
  """
  This fixture adds a teardown function that deletes continuous monitor logs from the current
  working directory after each test case is run.
  """
  def delete_continuous_monitor_logs():
    current_dir = os.getcwd()
    for filename in os.listdir(current_dir):
      if CONTINUOUS_MONITOR_PREFIX in filename:
        shutil.rmtree(os.path.join(current_dir, filename))

  request.addfinalizer(delete_continuous_monitor_logs)
  return None


def run_with_conf_and_verify(conf_filename, verification_func):
  """Verifies that a workload simulation executes correctly.

  Args:
    conf_filename: The path to a configuration file defining the workload to simulate. Assumed to
      be contained within a "conf" directory that is colocated with this file.
    verification_func: A function that accepts a SimulationConf object and a finished Simulator and
      verifies that the simulator correctly simulated the workload defined in the SimulationConf.
  """
  log_dir = "%s_%s" % (CONTINUOUS_MONITOR_PREFIX, datetime.now())
  if path.exists(log_dir):
    raise Exception("Continuous monitor log directory already exists: %s" % log_dir)
  else:
    os.makedirs(log_dir)

  conf_path = path.join("conf", conf_filename)
  conf_for_test = simulation_conf.SimulationConf(conf_path)
  # Copy conf_for_test for use during the verification stage because the internal data structures in
  # conf_for_test will be cleared while running the Simulator.
  conf_for_verification = copy.deepcopy(conf_for_test)

  sim = simulator.Simulator(conf_for_test, log_dir)
  try:
    sim.run(log_interval_ms=10)
  finally:
    sim.cleanup()

  verification_func(conf_for_verification, sim)


def test_two_workers_all_data_on_disk_with_shuffle(cleanup):
  """A pytest test case that validates a simple on-disk shuffle Job.

  Verifies that the Simulator operates correctly for a cluster with two Workers executing a
  two-Stage Job where all input, shuffle, and output data is stored on disk.

  Args:
    cleanup: A fixture that registers a cleanup function that removes the continuous monitor log
      files after the test case has finished.
  """
  run_with_conf_and_verify("on_disk_shuffle.xml", verify_two_workers_all_data_on_disk_with_shuffle)


def verify_two_workers_all_data_on_disk_with_shuffle(conf, sim):
  """Validates the results of the simple on-disk shuffle test case.

  Verifies that the provided Simulator reported the correct Job completion time for the Job
  specified by the provided SimulationConf. Also verifies that the simulation shuffled the correct
  amount of data. Assumes that the shuffle data was evenly distributed.
  """
  # Extract the Stage 0 Monotasks.
  stage_0_disk_read_monotask = get_disk_read_monotask_for_stage_from_conf(conf, stage_num=0)
  stage_0_compute_monotask = get_compute_monotask_for_stage_from_conf(conf, stage_num=0)
  stage_0_disk_write_monotask = get_disk_write_monotask_for_stage_from_conf(conf, stage_num=0)

  # The Workers are configured to only have one disk.
  disk_id = conf.disks.keys()[0]
  disk_write_throughput_Bpms = conf.get_throughput_Bpms_for_disk(disk_id, is_write=True)
  disk_read_throughput_Bpms = conf.get_throughput_Bpms_for_disk(disk_id, is_write=False)

  # Calculate the Stage 0 Monotask times.
  stage_0_disk_read_ms = (
    float(stage_0_disk_read_monotask.data_size_bytes) / disk_read_throughput_Bpms)
  stage_0_compute_ms = stage_0_compute_monotask.compute_time_ms
  stage_0_disk_write_ms = (
    float(stage_0_disk_write_monotask.data_size_bytes) / disk_write_throughput_Bpms)

  # Extract the Stage 1 Monotasks.
  stage_1_compute_monotask = get_compute_monotask_for_stage_from_conf(conf, stage_num=1)
  stage_1_disk_write_monotask = get_disk_write_monotask_for_stage_from_conf(conf, stage_num=1)

  # Calculate the Stage 1 Monotask times.
  stage_1_compute_ms = stage_1_compute_monotask.compute_time_ms
  stage_1_disk_write_ms = (
    float(stage_1_disk_write_monotask.data_size_bytes) / disk_write_throughput_Bpms)

  network_latency_ms = conf.network_latency_ms
  shuffle_bytes_per_worker = (float(stage_1_compute_monotask.shuffle_bytes_to_read) /
    conf.num_workers)
  # Time to fetch shuffle data from the local disk.
  shuffle_disk_read_ms = float(shuffle_bytes_per_worker) / disk_read_throughput_Bpms
  max_packet_size_bytes = task_constructs.Packet.max_size_bytes

  if shuffle_disk_read_ms > network_latency_ms:
    # If it takes longer to read the local shuffle data from disk than it does for the remote
    # shuffle request to arrive, then the disk read Monotask created by the remote request will need
    # to wait for the local shuffle data disk read to finish.
    disk_wait_ms = shuffle_disk_read_ms - network_latency_ms
  else:
    disk_wait_ms = 0

  # When transmitting a NetworkResponseMonotask, every Packet passes through the sending Worker's
  # network output queue and the receiving Worker's network input queue. In order to calculate the
  # total time for all of the Packets that make up a NetworkResponseMonotask to pass through these
  # two queues, consider the following table, which describes how Packets A and B are transmitted
  # across the network:
  #
  # The table ignores network latency and assumes that this is the only flow arriving at B. Since A
  # is the first Packet in the flow, its size is equal to the maximum size of a Packet.
  #
  #         +---------------------------------------------+
  #         | Phases encountered during network transfer  |
  #         +----------------------+----------------------+
  #         | src network output Q | dst network input Q  |
  # +-------+---------+------------+-----------+----------+--------------------+
  # | State | Waiting |  Sending   | Receiving | Received | Time in this state |
  # +-------+---------+------------+-----------+----------+--------------------+
  # |  T0   |  B, A   |            |           |          | 0                  |
  # |  T1   |     B   |     A      |           |          | Time to transmit A |
  # |  T2   |         |     B      |     A     |          | Time to transmit A |
  # |  T3   |         |            |     B     |      A   | Time to transmit B |
  # |  T4   |         |            |           |   B, A   | N/A                |
  # +-------+---------+------------+-----------+----------+--------------------+
  #
  # Therefore, the amount of time to transmit Packets A and B is:
  #   transmission time = (flow size + size of largest packet) / network bandwidth
  # This holds for flows of any size.
  if shuffle_bytes_per_worker < max_packet_size_bytes:
    largest_packet_size_bytes = shuffle_bytes_per_worker
  else:
    largest_packet_size_bytes = max_packet_size_bytes
  shuffle_transmission_ms = (float(shuffle_bytes_per_worker + largest_packet_size_bytes) /
    conf.get_network_bandwidth_Bpms())

  # Calculate the duration of the shuffle phase. Since we control the parameters of this test case,
  # we can make certain guarantees about when Monotasks are executed. Since there is only one Job
  # running, we know that when the local disk reads are submitted, they will not need to wait (all
  # disks will be idle since the Stage has just started). Since we know that there is only one disk
  # per Worker and all of the disks in the cluster have the same read throughput, the disk read
  # portions of fetching the local and remote shuffle data will take the same amount of time.
  # Therefore, fetching the remote shuffle data is guaranteed to be the critical path of the shuffle
  # phase because it involves the same disk read as fetching the local shuffle data but also
  # includes the time to send the data over the network. The remote read may also need to wait for a
  # local shuffle read to finish before accessing the disk, which increases the critical path.
  shuffle_critical_path_ms = (
    network_latency_ms +      # Time to send shuffle request
    disk_wait_ms +            # Time to wait for local shuffle read to finish
    shuffle_disk_read_ms +    # Time to read the shuffle data from disk
    network_latency_ms +      # Time to receive shuffle data (propogation delay)
    shuffle_transmission_ms)  # Time to receive shuffle data (transmission delay)

  stage_0_expected_ms = (
    network_latency_ms +        # Time to send Stage 0's Macrotasks to the Workers
    stage_0_disk_read_ms +      # Time to read the input data from disk
    stage_0_compute_ms +        # Time to execute Stage 0's compute phase
    stage_0_disk_write_ms +     # Time to write the shuffle data to disk
    network_latency_ms)        # Time to notify the master that Stage 0's Macrotasks completed
  stage_1_expected_ms = (
    network_latency_ms +        # Time to send Stage 1's Macrotasks to the Workers
    shuffle_critical_path_ms +  # Time to receive all of the shuffle data.
    stage_1_compute_ms +        # Time to execute Stage 1's compute phase
    stage_1_disk_write_ms +     # Time to write Stage 1's output data to disk
    network_latency_ms)         # Time to notify the master that Stage 2's Macrotasks completed
  expected_jct_ms = stage_0_expected_ms + stage_1_expected_ms

  logging.info(
    "Expected durations:\n\tStage 0: %s ms\n\tShuffle: %s ms\n\tStage 1: %s ms\n\tTotal: %s ms",
    stage_0_expected_ms,
    shuffle_critical_path_ms,
    stage_1_expected_ms,
    expected_jct_ms)

  actual_jct_ms = sim.jcts[conf.jobs[0].job_id]
  # Round both the actual and expected results to get rid of floating point rounding errors.
  assert abs(actual_jct_ms - expected_jct_ms) < 0.0000001
  verify_balanced_shuffle(conf, sim)


def test_two_workers_all_data_in_memory_with_shuffle(cleanup):
  """A pytest test case that validates a simple in-memory shuffle.

  Verifies that the Simulator operates correctly for a cluster with two Workers executing a
  two-Stage Job where all input, shuffle, and output data is stored in memory.

  Args:
    cleanup: A fixture that registers a cleanup function that removes the continuous monitor log
      files after the test case has finished.
  """
  run_with_conf_and_verify(
    "in_memory_shuffle.xml", verify_two_workers_all_data_in_memory_with_shuffle)


def verify_two_workers_all_data_in_memory_with_shuffle(conf, sim):
  """Validates the results of the simple in-memory shuffle test case.

  Verifies that the provided Simulator reported the correct Job completion time for the Job
  specified by the provided SimulationConf. Also verifies that the simulation shuffled the correct
  amount of data. Assumes that the shuffle data was evenly distributed.
  """
  # Extract the Stage 0 Monotasks.
  stage_0_compute_monotask = get_compute_monotask_for_stage_from_conf(conf, 0)
  stage_0_compute_ms = stage_0_compute_monotask.compute_time_ms

  # Extract the Stage 1 Monotasks.
  stage_1_compute_monotask = get_compute_monotask_for_stage_from_conf(conf, 1)
  stage_1_compute_ms = stage_1_compute_monotask.compute_time_ms

  shuffle_bytes_per_worker = (float(stage_1_compute_monotask.shuffle_bytes_to_read) /
    conf.num_workers)
  max_packet_size_bytes = task_constructs.Packet.max_size_bytes

  # See comment in verify_two_workers_all_data_on_disk_with_shuffle().
  if shuffle_bytes_per_worker < max_packet_size_bytes:
    largest_packet_size_bytes = shuffle_bytes_per_worker
  else:
    largest_packet_size_bytes = max_packet_size_bytes
  shuffle_transmission_ms = (float(shuffle_bytes_per_worker + largest_packet_size_bytes) /
    conf.get_network_bandwidth_Bpms())

  network_latency_ms = conf.network_latency_ms
  # Time to fetch shuffle data from the remote Worker.
  shuffle_ms = (
    network_latency_ms +      # Time to send shuffle request
    network_latency_ms +      # Time to receive shuffle data (propogation delay)
    shuffle_transmission_ms)  # Time to receive shuffle data (transmission delay)

  stage_0_expected_ms = (
    network_latency_ms +  # Time to send Stage 1's Macrotask to Worker
    stage_0_compute_ms +  # Time to execute Stage 1's compute phase
    network_latency_ms)  # Time to notify master that Stage 1's Macrotask completed
  stage_1_expected_ms = (
    network_latency_ms +  # Time to send Stage 2's Macrotask to Worker
    shuffle_ms +          # Time to receive all of the shuffle data.
    stage_1_compute_ms +  # Time to execute Stage 2's compute phase
    network_latency_ms)   # Time to notify master that Stage 2's Macrotask completed
  expected_jct_ms = stage_0_expected_ms + stage_1_expected_ms

  logging.info(
    "Expected durations:\n\tStage 0: %s ms\n\tShuffle: %s ms\n\tStage 1: %s ms\n\tTotal: %s ms",
    stage_0_expected_ms,
    shuffle_ms,
    stage_1_expected_ms,
    expected_jct_ms)

  actual_jct_ms = sim.jcts[conf.jobs[0].job_id]
  # Round both the actual and expected results to get rid of floating point rounding errors.
  assert abs(actual_jct_ms - expected_jct_ms) < 0.0000001
  verify_balanced_shuffle(conf, sim)


def verify_balanced_shuffle(conf, sim):
  """Validates that the correct amount of data was shuffled.

  Verifies that each Worker in the provided Simulator sent and received the correct amount of data,
  as specified by the parameters in the provided SimulationConf. Assumes that the shuffle data was
  evenly distributed.
  """
  stage_1_compute_monotask = get_compute_monotask_for_stage_from_conf(conf, stage_num=1)
  num_workers = conf.num_workers

  # The total amount of shuffle data that a Worker will request from all Workers, including itself.
  shuffle_bytes_to_read = stage_1_compute_monotask.shuffle_bytes_to_read
  # The total amount of shuffle data that a Worker will request from other Workers.
  expected_total_shuffle_bytes_requested_per_worker = (
    shuffle_bytes_to_read * (float(num_workers - 1) / num_workers))

  for worker in sim.workers:
    # Since the shuffle data is evenly distributed, each Worker should have sent and received the
    # same amount of data.
    assert worker.total_bytes_sent == expected_total_shuffle_bytes_requested_per_worker
    assert worker.total_bytes_received == expected_total_shuffle_bytes_requested_per_worker


def get_monotasks_for_stage_from_conf(conf, stage_num):
  """Extracts a Stage's Monotasks from a SimulatorConf.

  Returns:
    A list of Monotasks for one Macrotask from the Stage with the provided index, extracted from the
    provided SimulatorConf.
  """
  return conf.jobs[0].waiting_stages[stage_num].waiting_macrotasks[0].remaining_monotasks


def get_compute_monotask_for_stage_from_conf(conf, stage_num):
  """Extracts a Stage's ComputeMonotask from a SimulatorConf.

  Returns:
    A ComputeMonotask belonging to one of the Macrotasks in the Stage with the provided index,
    extracted from the provided SimulatorConf.
  """
  return [monotask
    for monotask in get_monotasks_for_stage_from_conf(conf, stage_num)
    if isinstance(monotask, task_constructs.ComputeMonotask)][0]


def get_disk_read_monotask_for_stage_from_conf(conf, stage_num):
  """Extracts a Stage's disk read Monotask from a SimulatorConf.

  Returns:
    A disk read Monotask belonging to one of the Macrotasks in the Stage with the provided index,
    extracted from the provided SimulatorConf.
  """
  return [monotask
    for monotask in get_monotasks_for_stage_from_conf(conf, stage_num)
    if isinstance(monotask, task_constructs.DiskMonotask) and not monotask.is_write][0]


def get_disk_write_monotask_for_stage_from_conf(conf, stage_num):
  """Extracts a Stage's disk write Monotask from a SimulatorConf.

  Returns:
    A disk write Monotask belonging to one of the Macrotasks in the Stage with the provided index,
    extracted from the provided SimulatorConf.
  """
  return [monotask
    for monotask in get_monotasks_for_stage_from_conf(conf, stage_num)
    if isinstance(monotask, task_constructs.DiskMonotask) and monotask.is_write][0]


def get_num_macrotasks_in_stage(conf, stage_num):
  """Finds the number of Macrotasks in a Stage.

  Returns:
    The number of Macrotasks in the Stage with the provided index, extracted from the provided
    SimulatorConf.
  """
  return len(conf.jobs[0].waiting_stages[stage_num].waiting_macrotasks)
