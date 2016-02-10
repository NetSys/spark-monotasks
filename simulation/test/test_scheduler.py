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
"""Contains unit tests of the Monotasks Simulator's Scheduler module.

Run the tests in this file by executing the command "py.test test_scheduler.py" from the containing
directory.
"""

import logging
from os import path

from simulation import events
from simulation import simulation_conf
from simulation import simulator
from simulation import task_constructs
from simulation import worker

# Set the logging level to INFO so that logs are printed if a test fails.
logging.basicConfig(level=logging.INFO)


def test_fixed_slots_scheduler_handle_monotask_end(tmpdir):
  """A pytest test case that validates the FixedSlotsScheduler.

  Verifies that the FixedSlotsScheduler operates correctly when it is notified that a Monotask has
  completed.

  Args:
    tmpdir: A built-in pytest fixture used to create a temporary directory in which to store
      continuous monitor files.
  """
  scheduler = get_scheduler_for_conf(tmpdir, conf_filename="fixed_slots_scheduler.xml")

  current_time_ms = 0L
  macrotask = task_constructs.Macrotask(task_constructs.Stage(task_constructs.Job()))
  first_monotask = task_constructs.ComputeMonotask(
    macrotask,
    compute_time_ms=0,
    shuffle_bytes_to_read=0,
    is_shuffle_data_on_disk=False,
    num_partitions_in_current_stage=1)
  second_monotask = task_constructs.ComputeMonotask(
    macrotask,
    compute_time_ms=0,
    shuffle_bytes_to_read=0,
    is_shuffle_data_on_disk=False,
    num_partitions_in_current_stage=1)

  first_monotask.is_finished = True

  new_events = scheduler.handle_monotask_end(current_time_ms, first_monotask)
  assert len(new_events) == 0

  second_monotask.is_finished = True
  new_events = scheduler.handle_monotask_end(current_time_ms, second_monotask)
  verify_macrotask_request(current_time_ms, scheduler, new_events)


def test_throttling_scheduler_internals(tmpdir):
  """A pytest test case verifies the low-level operation of the ThrottlingScheduler.

  This test case ensures that the ThrottlingScheduler's internal values are updated correctly.

  The resource phase pipeline under test looks like:
    Macrotask Request Phase -> Network Phase -> Compute Phase (1 core) (bottleneck)

  Args:
    tmpdir: A built-in pytest fixture used to create a temporary directory in which to store
      continuous monitor files.
  """
  current_time_ms = 0L
  scheduler = get_scheduler_for_conf(
    tmpdir, conf_filename="throttling_scheduler_network_compute.xml")
  worker_node = scheduler.worker
  sim = worker_node.simulator
  (first_stage, second_stage) = sim.current_job.stages
  first_stage_macrotasks = first_stage.macrotasks
  second_stage_macrotasks = second_stage.macrotasks

  num_first_stage_macrotasks = len(first_stage_macrotasks)
  all_workers = sim.workers
  num_workers = len(all_workers)
  # Assign each Macrotask from the first Stage to a Worker, so that the logic for creating Monotasks
  # to read the shuffle data know how much shuffle data to read from each Worker.
  for i in xrange(num_first_stage_macrotasks):
    first_stage_macrotasks[i].worker = all_workers[i % num_workers]
  for macrotask in second_stage_macrotasks:
    macrotask.worker = worker_node

  (first_macrotask, second_macrotask, third_macrotask, fourth_macrotask) = second_stage_macrotasks

  # The first Macrotask starts.
  first_compute_monotask = get_compute_monotask_from_macrotask(first_macrotask)
  verify_macrotask_start(
    current_time_ms,
    scheduler,
    first_macrotask,
    expected_phase_pipeline_state=[2, 1, 2, True, 0, 1, False, 0, 0, False])

  # The second Macrotask starts.
  second_compute_monotask = get_compute_monotask_from_macrotask(second_macrotask)
  verify_macrotask_start(
    current_time_ms,
    scheduler,
    second_macrotask,
    expected_phase_pipeline_state=[2, 2, 2, True, 0, 1, False, 0, 0, False])

  # The second Macrotask finishes the network Phase. The third is requested.
  first_network_monotask = get_network_request_monotask_from_macrotask(first_macrotask)
  verify_monotask_end(
    current_time_ms,
    scheduler,
    first_network_monotask,
    expect_macrotask_request=True,
    expected_phase_pipeline_state=[3, 2, 3, True, 1, 2, False, 0, 1, False])

  # The third Macrotask starts.
  third_compute_monotask = get_compute_monotask_from_macrotask(third_macrotask)
  verify_macrotask_start(
    current_time_ms,
    scheduler,
    third_macrotask,
    expected_phase_pipeline_state=[3, 3, 3, True, 1, 2, False, 0, 1, False])

  # The second Macrotask finishes the network Phase.
  second_network_monotask = get_network_request_monotask_from_macrotask(second_macrotask)
  verify_monotask_end(
    current_time_ms,
    scheduler,
    second_network_monotask,
    expect_macrotask_request=False,
    expected_phase_pipeline_state=[3, 3, 3, True, 2, 2, True, 0, 1, False])

  # The first Macrotask finishes the compute Phase. The fourth is requested.
  verify_monotask_end(
    current_time_ms,
    scheduler,
    first_compute_monotask,
    expect_macrotask_request=True,
    expected_phase_pipeline_state=[4, 3, 4, True, 2, 3, True, 1, 2, False])

  # The fourth Macrotask starts.
  fourth_compute_monotask = get_compute_monotask_from_macrotask(fourth_macrotask)
  verify_macrotask_start(
    current_time_ms,
    scheduler,
    fourth_macrotask,
    expected_phase_pipeline_state=[4, 4, 4, True, 2, 3, True, 1, 2, False])

  # The third Macrotask finishes the network Phase.
  third_network_monotask = get_network_request_monotask_from_macrotask(third_macrotask)
  verify_monotask_end(
    current_time_ms,
    scheduler,
    third_network_monotask,
    expect_macrotask_request=False,
    expected_phase_pipeline_state=[4, 4, 4, True, 3, 3, True, 1, 2, False])

  # The second Macrotask finishes the compute Phase. Another is requested, but there are no more to
  # send.
  verify_monotask_end(
    current_time_ms,
    scheduler,
    second_compute_monotask,
    expect_macrotask_request=True,
    expected_phase_pipeline_state=[5, 4, 5, True, 3, 4, True, 2, 3, False])

  # The fourth Macrotask finishes the network Phase.
  fourth_network_monotask = get_network_request_monotask_from_macrotask(fourth_macrotask)
  verify_monotask_end(
    current_time_ms,
    scheduler,
    fourth_network_monotask,
    expect_macrotask_request=False,
    expected_phase_pipeline_state=[5, 4, 5, True, 4, 4, True, 2, 3, False])

  # The third Macrotask finishes the compute Phase. Another is requested, but there are no more to
  # send.
  verify_monotask_end(
    current_time_ms,
    scheduler,
    third_compute_monotask,
    expect_macrotask_request=True,
    expected_phase_pipeline_state=[6, 4, 6, True, 4, 4, False, 3, 4, False])

  # The fourth Macrotask finishes the compute Phase.
  verify_monotask_end(
    current_time_ms,
    scheduler,
    fourth_compute_monotask,
    expect_macrotask_request=False,
    expected_phase_pipeline_state=[6, 4, 6, True, 4, 4, False, 4, 4, False])

  for worker_node in all_workers:
    worker_node.continuous_monitor.close()


def test_throttling_scheduler_with_concurrency(tmpdir):
  """A pytest test case that checks if the ThrottlingScheduler correctly handles concurrency.

  This test case validates the end-to-end operation of the ThrottlingScheduler when managing a
  Worker with resources that have a concurency greater than one.

  The resource phase pipeline under test looks like:
    Macrotask Request Phase -> Disk (2 disks) -> Compute Phase (4 cores) (bottleneck)

  Args:
    tmpdir: A built-in pytest fixture used to create a temporary directory in which to store
      continuous monitor files.
  """
  current_time_ms = 0L
  scheduler = get_scheduler_for_conf(tmpdir, conf_filename="throttling_scheduler_disk_compute.xml")
  worker_node = scheduler.worker
  macrotasks = worker_node.simulator.current_job.stages[0].macrotasks
  disk_monotasks = []
  compute_monotasks = []

  for i in xrange(10):
    disk_monotasks.append(get_disk_monotask_from_macrotask(macrotasks[i]))
    compute_monotasks.append(get_compute_monotask_from_macrotask(macrotasks[i]))

    # Start the first five Macrotasks.
    if i < 5:
      scheduler.handle_macrotask_start(macrotasks[i])

  # The first and second Macrotasks finish the disk Phase.
  verify_monotask_end(current_time_ms, scheduler, disk_monotasks[0], expect_macrotask_request=False)
  verify_monotask_end(current_time_ms, scheduler, disk_monotasks[1], expect_macrotask_request=False)

  # The third and fourth Macrotasks finish the disk Phase. The sixth and seventh should be
  # requested.
  verify_monotask_end(current_time_ms, scheduler, disk_monotasks[2], expect_macrotask_request=True)
  verify_monotask_end(current_time_ms, scheduler, disk_monotasks[3], expect_macrotask_request=True)

  # The sixth and seventh Macrotasks are received from the master.
  scheduler.handle_macrotask_start(macrotasks[5])
  scheduler.handle_macrotask_start(macrotasks[6])

  # The fifth Macrotask finishes the disk Phase. The eighth should be requested.
  verify_monotask_end(current_time_ms, scheduler, disk_monotasks[4], expect_macrotask_request=True)

  # The sixth Macrotask finishes the disk Phase.
  verify_monotask_end(current_time_ms, scheduler, disk_monotasks[5], expect_macrotask_request=False)

  # The eighth Macrotask is received from the master.
  scheduler.handle_macrotask_start(macrotasks[7])

  # The eighth Macrotask finishes the disk Phase.
  verify_monotask_end(current_time_ms, scheduler, disk_monotasks[6], expect_macrotask_request=False)

  # The first Macrotask finishes the compute Phase.
  verify_monotask_end(
    current_time_ms, scheduler, compute_monotasks[0], expect_macrotask_request=False)

  # The second and third Macrotasks finish the compute Phase. The ninth and tenth should be
  # requested.
  verify_monotask_end(
    current_time_ms, scheduler, compute_monotasks[1], expect_macrotask_request=True)
  verify_monotask_end(
    current_time_ms, scheduler, compute_monotasks[2], expect_macrotask_request=True)

  # The fourth Macrotask finishes the compute Phase.
  verify_monotask_end(
    current_time_ms, scheduler, compute_monotasks[3], expect_macrotask_request=False)

  worker_node.continuous_monitor.close()


def get_compute_monotask_from_macrotask(macrotask):
  """ Returns a ComputeMonotask from the provided Macrotask. """
  return get_monotask_from_macrotask(task_constructs.ComputeMonotask, macrotask)


def get_network_request_monotask_from_macrotask(macrotask):
  """ Returns a NetworkRequestMonotask from the provided Macrotask. """
  return get_monotask_from_macrotask(task_constructs.NetworkRequestMonotask, macrotask)


def get_disk_monotask_from_macrotask(macrotask):
  """ Returns a DiskMonotask from the provided Macrotask. """
  return get_monotask_from_macrotask(task_constructs.DiskMonotask, macrotask)


def get_monotask_from_macrotask(monotask_type, macrotask):
  """ Returns a Monotask of the specified type from the provided Macrotask. """
  return next((monotask for monotask in macrotask.monotasks if isinstance(monotask, monotask_type)))


def verify_macrotask_start(
    current_time_ms,
    scheduler,
    macrotask,
    expected_phase_pipeline_state):
  """Verifies that a Scheduler reacts correctly when a Macrotask arrives at its Worker.

  Informs the provided Scheduler that the specified Macrotask has arrived at its Worker and verifies
  that the Scheduler's Phase pipeline's internal state is correct.
  """
  # Create any Monotasks required to read shuffle data so that the Scheduler knows about them.
  get_compute_monotask_from_macrotask(macrotask).create_monotasks_for_shuffle(
    current_time_ms, scheduler.worker.simulator.workers)
  scheduler.handle_macrotask_start(macrotask)
  verify_phase_pipeline_state(scheduler, expected_phase_pipeline_state)


def verify_monotask_end(
    current_time_ms,
    scheduler,
    monotask,
    expect_macrotask_request,
    expected_phase_pipeline_state=None):
  """Verifies that a Scheduler reacts correctly when a Monotask finishes.

  Informs the provided Scheduler that the specified Monotask has finished and checks whether a
  MacrotaskRequest Event is returned or not. Optionally verifies that the Scheduler's Phase
  pipeline's internal state is correct.
  """
  new_events = scheduler.handle_monotask_end(current_time_ms, monotask)
  if expect_macrotask_request:
    verify_macrotask_request(current_time_ms, scheduler, new_events)
  else:
    assert len(new_events) == 0

  if expected_phase_pipeline_state is not None:
    verify_phase_pipeline_state(scheduler, expected_phase_pipeline_state)


def verify_macrotask_request(current_time_ms, scheduler, event_list):
  """Verifies that the provided list of Events contains a single MacrotaskRequest Event.

  Also checks that the MacrotaskRequest Event is scheduled for the correct time.
  """
  assert len(event_list) == 1
  arrival_time_ms, macrotask_request_event = event_list[0]
  assert arrival_time_ms == current_time_ms + scheduler.worker.conf.network_latency_ms
  assert isinstance(macrotask_request_event, events.MacrotaskRequest)
  assert macrotask_request_event.worker is scheduler.worker


def verify_phase_pipeline_state(scheduler, expected_phase_pipeline_state):
  """Verifies that a Scheduler's Phase pipeline's internal state is correct.

  Args:
    scheduler: The Scheduler to check.
    expected_phase_pipeline_state: A list of the internal state values to expect. The first element
      should be the number of Macrotasks that should have been requested from the master node. For
      the ith Phase in the pipeline, the expected values for its "num_finished",
      "num_approved_to_start", and "is_throttled" variables should be located at indices 3 * i + 1,
      3 * i + 2, and 3 * i + 3, respectively.
  """
  first_phase = scheduler.first_phase
  current_phase = first_phase

  # Count the number of phases in order to verify that expected_phase_pipeline_state is properly
  # formed before starting the verification step.
  num_phases = 0
  while current_phase is not None:
    num_phases += 1
    current_phase = current_phase.next_phase
  num_expected_values = len(expected_phase_pipeline_state)
  expected_num_expected_values = 3 * num_phases + 1
  if num_expected_values != expected_num_expected_values:
    raise ValueError(("expected_phase_pipeline_state (%s) is not properly formed. " +
      "Expected length: %s, Actual length: %s") %
        (expected_phase_pipeline_state, expected_num_expected_values, num_expected_values))

  current_phase = first_phase
  i = 0
  while current_phase is not None:
    logging.info("Checking %s", current_phase)

    if i == 0:
      assert current_phase.num_requested_from_master == expected_phase_pipeline_state[0]
    assert current_phase.num_finished == expected_phase_pipeline_state[3 * i + 1]
    assert current_phase.num_approved_to_start == expected_phase_pipeline_state[3 * i + 2]
    assert current_phase.is_throttled == expected_phase_pipeline_state[3 * i + 3]

    current_phase = current_phase.next_phase
    i += 1


def get_scheduler_for_conf(tmpdir, conf_filename):
  """Creates a Scheduler as specified by a configuration file.

  Args:
    tmpdir: The path to a temporary directory where the Scheduler's Worker can store its
      continuous monitor file. The caller is responsible for cleaning up this directory.
    conf_filename: The name of a configuration file specifying what type of Scheduler to create. The
      configuration file is expected to be contained within a "conf" directory that is colocated
      with this file.

  Returns:
    A Scheduler as specified by the provided configuration file. The Scheduler's Worker will be
    configured to know about a Simulator instance.
  """
  conf_path = path.join(path.dirname(path.realpath(__file__)), "conf", conf_filename)
  sim = simulator.Simulator(simulation_conf.XMLSimulationConf(conf_path), str(tmpdir))

  if len(sim.jobs) > 0:
    sim.current_job = sim.jobs[0]

  return sim.workers[0].scheduler
