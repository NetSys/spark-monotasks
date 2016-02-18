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

import abc
import logging
import random


class Job(object):
  """ A Job consists of multiple Stages that are executed sequentially. """

  __next_id = 0

  def __init__(self):
    self.job_id = Job.__new_id()
    # A list of the Stages that make up this Job. Stages must be executed sequentially. Populated by
    # Stages as they are created.
    self.stages = []
    # The time (in ms) at which this Job started executing. Set by the Simulator. Used by the
    # Simulator to calculate Job completion time.
    self.start_time_ms = 0

  def __repr__(self):
    return "(Job | id: %s)" % self.job_id

  @staticmethod
  def __new_id():
    """ Returns a new globally-unique Job ID . """
    old_id = Job.__next_id
    Job.__next_id += 1
    return old_id

  def is_finished(self):
    """Checks if this Job has finished.

    Returns:
      True if all of the Stages in this Job have finished, or False otherwise.
    """
    return self.get_next_stage() is None

  def get_next_stage(self):
    """Retrieves the next Stage to execute.

    Returns:
      The first Stage in this Job that has not finished, or None if all Stages have finished.
    """
    return next((stage for stage in self.stages if not stage.is_finished()), None)

  def calculate_ideal_completion_time_ms(self, conf, output_file):
    """
    Returns the ideal completion time of this Job (in ms). Writes the ideal resources times of each
    Stage to the provided file.
    """
    return sum(
      [stage.calculate_ideal_completion_time_ms(conf, output_file) for stage in self.stages])


class Stage(object):
  """ A Stage consists of multiple Macrotasks that are executed in parallel by many Workers. """

  __next_id = 0

  def __init__(self, job):
    self.stage_id = Stage.__new_id()
    self.job = job
    self.job.stages.append(self)
    # A list of the Macrotasks that make up this Stage. Macrotasks are executed in parallel by
    # Workers. Populated by Macrotasks as they are created.
    self.macrotasks = []

  def __repr__(self):
    return "(Stage | id: %s)" % self.stage_id

  @staticmethod
  def __new_id():
    """ Returns a new globally-unique Stage ID. """
    old_id = Stage.__next_id
    Stage.__next_id += 1
    return old_id

  def is_finished(self):
    """Checks if this Stage has finished.

    Returns:
      True if all of the Macrotasks in this Stage have finished, or False otherwise.
    """
    return all(macrotask.master_knows_is_finished for macrotask in self.macrotasks)

  def get_next_macrotask(self):
    """Retrieves the next Macrotask to execute.

    Returns:
      A Macrotask in this Stage that has not been assigned to a Worker, or None if all Macrotasks
      have been assigned to Workers.
    """
    return next((macrotask for macrotask in self.macrotasks if macrotask.worker is None), None)

  def calculate_ideal_completion_time_ms(self, conf, output_file):
    """Calculates the time that this Stage should take to finish in an ideal system.

    Writes this Stage's ideal resource times to the provided file.

    Returns:
      The ideal completion time of this Stage (in ms).
    """
    total_compute_time_ms = 0
    total_network_bytes = 0
    total_disk_write_bytes = 0
    total_disk_read_bytes = 0

    for macrotask in self.macrotasks:
      (macrotask_compute_time_ms,
        macrotask_network_bytes,
        macrotask_disk_write_bytes,
        macrotask_disk_read_bytes) = macrotask.get_resource_usage()
      total_compute_time_ms += macrotask_compute_time_ms
      total_network_bytes += macrotask_network_bytes
      total_disk_write_bytes += macrotask_disk_write_bytes
      total_disk_read_bytes += macrotask_disk_read_bytes

    num_workers = conf.num_workers
    total_cores = num_workers * conf.num_cores
    # Ideally, the total compute time will be distributed evenly across all of the cores in the
    # cluster.
    parallel_compute_time_ms = total_compute_time_ms / total_cores

    # Ideally, the responsibility of sending shuffle data will be distributed evenly across the
    # Workers in the cluster.
    network_bytes_per_worker = total_network_bytes / num_workers
    max_packet_size_bytes = Packet.max_size_bytes
    if network_bytes_per_worker < max_packet_size_bytes:
      largest_packet_size_bytes = network_bytes_per_worker
    else:
      largest_packet_size_bytes = max_packet_size_bytes
    # Since we model the links connecting each Worker to the network separately, we need to add the
    # time to transmit one Packet in order to account for the fact that Packets traverse two links
    # when traveling between Workers.
    parallel_network_time_ms = ((network_bytes_per_worker + largest_packet_size_bytes) /
      conf.network_bandwidth_Bpms)

    # Compute the total disk write and read capabilities of the cluster.
    total_disk_write_throughput_Bpms = total_disk_read_throughput_Bpms = 0
    for disk_write_throughput_Bpms, disk_read_throughput_Bpms in conf.disks.itervalues():
      total_disk_write_throughput_Bpms += num_workers * disk_write_throughput_Bpms
      total_disk_read_throughput_Bpms += num_workers * disk_read_throughput_Bpms
    # Ideally, moving data to and from disk will be load balanced across the disks in the cluster.
    if total_disk_write_throughput_Bpms == 0:
      parallel_disk_write_time_ms = 0
    else:
      parallel_disk_write_time_ms = total_disk_write_bytes / total_disk_write_throughput_Bpms
    if total_disk_read_throughput_Bpms == 0:
      parallel_disk_read_time_ms = 0
    else:
      parallel_disk_read_time_ms = total_disk_read_bytes / total_disk_read_throughput_Bpms
    parallel_disk_time_ms = parallel_disk_write_time_ms + parallel_disk_read_time_ms

    message = (("%s, %s Ideal Resource Times:\n  ideal CPU time: %.2f ms\n  " +
      "ideal network time: %.2f ms\n  ideal disk time: %.2f ms") % (self.job, self,
        parallel_compute_time_ms, parallel_network_time_ms, parallel_disk_time_ms))
    logging.info(message)
    output_file.write("%s\n\n" % message)
    return max(parallel_compute_time_ms, parallel_network_time_ms, parallel_disk_time_ms)


class Macrotask(object):
  """ A Macrotask consists of a DAG of Monotasks that are all executed by the same Worker. """

  __next_id = 0

  def __init__(self, stage):
    self.macrotask_id = Macrotask.__new_id()
    # The Stage to which this Macrotask belongs.
    self.stage = stage
    self.stage.macrotasks.append(self)
    # A list of the Monotasks that make up this Macrotask. Populated by Monotasks as they are
    # created.
    self.monotasks = []
    # The Worker to which this Macrotask has been assigned, or None if this Macrotask has not been
    # assigned to a Worker yet.
    self.worker = None
    # Whether the master node has been notified that this Macrotask has finished.
    self.master_knows_is_finished = False

  def __repr__(self):
    return "(Macrotask | id: %s)" % self.macrotask_id

  @staticmethod
  def __new_id():
    """ Returns a new globally-unique Macrotask ID. """
    old_id = Macrotask.__next_id
    Macrotask.__next_id += 1
    return old_id

  def all_monotasks_finished(self):
    """ Returns True if all of this Macrotask's Monotasks have finished, or False otherwise. """
    return all(monotask.is_finished for monotask in self.monotasks)

  def get_resource_usage(self):
    """Retrieves information about how much this Macrotask used each system resource.

    Returns:
      This Macrotask's total compute time (in ms), total bytes read over the network, total bytes
      written to disk, and total bytes read from disk.
    """
    total_compute_time_ms = 0
    total_network_bytes = 0
    total_disk_write_bytes = 0
    total_disk_read_bytes = 0

    for monotask in self.monotasks:
      if isinstance(monotask, ComputeMonotask):
        total_compute_time_ms += monotask.compute_time_ms
      elif isinstance(monotask, NetworkRequestMonotask):
        # NetworkRequestMonotasks do not use any system resources.
        continue
      elif isinstance(monotask, NetworkResponseMonotask):
        total_network_bytes += monotask.data_size_bytes
      elif isinstance(monotask, DiskMonotask):
        data_size_bytes = monotask.data_size_bytes
        if monotask.is_write:
          total_disk_write_bytes += data_size_bytes
        else:
          total_disk_read_bytes += data_size_bytes
      else:
        raise Exception("Unknown monotask type: %s" % monotask)

    return (total_compute_time_ms, total_network_bytes, total_disk_write_bytes,
      total_disk_read_bytes)

  def get_previous_stage(self):
    """
    Returns the Stage before the Stage to which this Macrotask belongs, or None if it is part of the
    first Stage in its Job.
    """
    all_stages = self.worker.simulator.current_job.stages
    for i in xrange(len(all_stages)):
      if all_stages[i] is self.stage:
        if i == 0:
          return None
        else:
          return all_stages[i - 1]

    raise Exception("Cannot find %s in the current Job" % self.stage)


class Monotask(object):
  """
  A Monotask is a unit of work that depends on other Monotasks and uses only one system resource.
  """

  __next_id = 0

  def __init__(self, macrotask):
    self.monotask_id = Monotask.__new_id()
    # The Macrotask to which this Monotask belongs.
    self.macrotask = macrotask
    self.macrotask.monotasks.append(self)
    # A list of Monotasks that must complete before this Monotask can be executed. Populated by
    # add_dependency().
    self.dependencies = []
    # A list of Monotasks that cannot be executed until this Monotask completes. Populated by
    # add_dependency().
    self.dependents = []
    self.is_finished = False

  @staticmethod
  def __new_id():
    """ Returns a new globally-unique Monotask ID. """
    old_id = Monotask.__next_id
    Monotask.__next_id += 1
    return old_id

  def log_start(self, current_time_ms, worker):
    logging.info("%s: Executing %s on %s", current_time_ms, self, worker)

  def add_dependency(self, new_dependency):
    """
    Adds the provided Monotask as a dependency of this Monotask, and this Monotask as a dependent of
    the provided Monotask.
    """
    self.dependencies.append(new_dependency)
    new_dependency.dependents.append(self)

  def add_dependencies(self, new_dependencies):
    """
    Adds the provided Monotasks as dependencies of this Monotask, and this Monotask as a dependent
    of each of the provided Monotasks.
    """
    for new_dependency in new_dependencies:
      self.add_dependency(new_dependency)

  def dependencies_have_finished(self):
    """Checks if this Monotask can be executed.

    Returns:
      True if all of this Monotask's dependencies have finished executing, or False otherwise.
    """
    return all(dependency.is_finished for dependency in self.dependencies)

  @abc.abstractmethod
  def schedule(self, current_time_ms, worker):
    """
    Passes this Monotask to the correct resource scheduler on the provided Worker. Returns a
    MonotaskEnd Event if this Monotask started executing.
    """

  def end(self, current_time_ms, worker):
    """
    Marks this Monotask as having finished. Also, notifies the provided Worker that this Monotask
    finished executing. Returns a MonotaskEnd Event if another Monotask started executing.
    """
    self.is_finished = True
    return self.update_worker(current_time_ms, worker)

  @abc.abstractmethod
  def update_worker(self, current_time_ms, worker):
    """
    Notifies the provided Worker that this Monotask finished executing. Returns a MonotaskEnd Event
    if another Monotask started executing.
    """


class ComputeMonotask(Monotask):
  """ A Monotask that uses one CPU core. """

  def __init__(
      self,
      macrotask,
      compute_time_ms,
      shuffle_bytes_to_read,
      is_shuffle_data_on_disk,
      num_partitions_in_current_stage):
    Monotask.__init__(self, macrotask)
    self.compute_time_ms = compute_time_ms
    # The total amount of shuffle data that must be retrieved before this ComputeMonotask can
    # execute. If this is positive, then this ComputeMonotask has a shuffle dependency.
    self.shuffle_bytes_to_read = shuffle_bytes_to_read
    # If this is True, then the shuffle data is stored on disk, otherwise it is stored in memory.
    self.is_shuffle_data_on_disk = is_shuffle_data_on_disk
    # The number of partitions in the current Stage. This is used to calculate how much shuffle data
    # to read from each Worker if this ComputeMonotask has a shuffle dependency.
    self.num_partitions_in_current_stage = num_partitions_in_current_stage

  def __repr__(self):
    return (
      ("(ComputeMonotask | id: %s | macrotask: %s | compute time: %s ms | " +
        "shuffle bytes to read: %s | is shuffle data on disk: %s)") % (
      self.monotask_id,
      self.macrotask.macrotask_id,
      self.compute_time_ms,
      self.shuffle_bytes_to_read,
      self.is_shuffle_data_on_disk))

  def schedule(self, current_time_ms, worker):
    return worker.schedule_compute(current_time_ms, self)

  def update_worker(self, current_time_ms, worker):
    return worker.compute_monotask_end(current_time_ms, self)

  def create_monotasks_for_shuffle(self, current_time_ms, all_workers):
    """
    If this ComputeMonotask has a shuffle dependency, creates a DiskMonotask to read shuffle data
    from the local machine (if the shuffle data is stored on disk) and NetworkRequestMonotasks to
    fetch shuffle data from the remote workers. Adds any new Monotasks to the appropriate
    Macrotask's list of Monotasks. Does not return anything.
    """
    if self.shuffle_bytes_to_read == 0:
      # This ComputeMonotask does not have a shuffle dependency.
      return []

    previous_stage_macrotasks = self.macrotask.get_previous_stage().macrotasks
    num_macrotasks_in_previous_stage = len(previous_stage_macrotasks)
    local_worker = self.macrotask.worker

    for remote_worker in all_workers:
      num_macrotasks_assigned_to_worker_in_previous_stage = len([macrotask
        for macrotask in previous_stage_macrotasks
        if macrotask.worker is remote_worker])
      # The amount of shuffle data to read from each Worker depends on the number of Macrotasks that
      # were assigned to that Worker in the previous Stage.
      shuffle_bytes_on_worker = self.shuffle_bytes_to_read * (
        float(num_macrotasks_assigned_to_worker_in_previous_stage) /
          num_macrotasks_in_previous_stage)

      if remote_worker is local_worker:
        if self.is_shuffle_data_on_disk:
          # Create a DiskMonotask to read the shuffle data from the local disk.
          new_monotask = DiskMonotask(self.macrotask, shuffle_bytes_on_worker, is_write=False)
          # TODO: We need a way to track which disk the block is on. For now, we just pick a disk
          #       uniformly at random.
          new_monotask.disk_id = random.choice(local_worker.disks.keys())
        else:
          # Since the shuffle data is already stored in memory, we do not need to create another
          # Monotask to read it.
          new_monotask = None
      else:
        # Create a NetworkRequestMonotask to retrieve the shuffle data from the remote Worker.
        new_monotask = NetworkRequestMonotask(
          self.macrotask,
          local_worker,
          remote_worker,
          shuffle_bytes_on_worker,
          self.is_shuffle_data_on_disk)

      if new_monotask is not None:
        logging.info(
          "%s: %s created %s to read shuffle data for %s",
          current_time_ms,
          local_worker,
          new_monotask,
          self)
        self.add_dependency(new_monotask)


class NetworkRequestMonotask(Monotask):
  """ A Monotask that notifies a Worker that another Worker requires some of its data. """

  def __init__(self, macrotask, src_worker, dst_worker, request_size_bytes, is_on_disk):
    Monotask.__init__(self, macrotask)
    self.src_worker = src_worker
    self.dst_worker = dst_worker
    self.request_size_bytes = request_size_bytes
    self.is_on_disk = is_on_disk

  def __repr__(self):
    return ("(NetworkRequestMonotask | id: %s | macrotask: %s | src, dst: %s, %s | " +
      "data: %s bytes | on disk: %s)") % (
        self.monotask_id,
        self.macrotask.macrotask_id,
        self.src_worker.worker_id,
        self.dst_worker.worker_id,
        self.request_size_bytes,
        self.is_on_disk)

  def schedule(self, current_time_ms, worker):
    self.log_start(current_time_ms, worker)
    return worker.schedule_network_request(current_time_ms, self)

  def update_worker(self, current_time_ms, worker):
    return worker.requested_data_received(self)


class NetworkResponseMonotask(Monotask):
  """
  A Monotask that transfers data from one Worker to another. Will be broken into Packets for
  transmission over the network. A NetworkResponseMonotask is considered finished when its last
  Packet departs its source Worker.
  """

  def __init__(self, macrotask, data_size_bytes, network_request_monotask):
    Monotask.__init__(self, macrotask)
    self.data_size_bytes = data_size_bytes
    self.network_request_monotask = network_request_monotask
    self.src_worker = network_request_monotask.dst_worker
    self.dst_worker = network_request_monotask.src_worker

    conf = self.src_worker.conf
    average_network_bandwidth_Bpms = conf.network_bandwidth_Bpms
    network_bandwidth_variance = conf.network_bandwidth_variance
    self.network_bandwidth_Bpms = random.uniform(
      average_network_bandwidth_Bpms * (1 - network_bandwidth_variance),
      average_network_bandwidth_Bpms * (1 + network_bandwidth_variance))

  def __repr__(self):
    return ("(NetworkResponseMonotask | id: %s | macrotask: %s | request id: %s | " +
      "src, dst: %s, %s | data: %s bytes)") % (
        self.monotask_id,
        self.macrotask.macrotask_id,
        self.network_request_monotask.monotask_id,
        self.src_worker.worker_id,
        self.dst_worker.worker_id,
        self.data_size_bytes)

  def schedule(self, current_time_ms, worker):
    self.log_start(current_time_ms, worker)
    return worker.schedule_network_response(current_time_ms, self)

  def update_worker(self, current_time_ms, worker):
    # NetworkResponseMonotasks do not need to inform either their source or destination Worker that
    # they completed.
    return []

  def get_packets(self, current_time_ms):
    """
    Returns a list of Packets that make up the data that this NetworkResponseMonotask will transfer.
    """
    max_packet_size_bytes = Packet.max_size_bytes
    bytes_remaining = self.data_size_bytes
    next_seq_num = 0
    packets = []
    while bytes_remaining > 0:
      if bytes_remaining <= max_packet_size_bytes:
        is_last = True
        packet_size_bytes = bytes_remaining
      else:
        is_last = False
        packet_size_bytes = max_packet_size_bytes

      packets.append(Packet(self, next_seq_num, is_last, packet_size_bytes))
      next_seq_num += 1
      bytes_remaining -= packet_size_bytes

    logging.debug("%s: Created %s packets for %s", current_time_ms, len(packets), self)
    return packets


class DiskMonotask(Monotask):
  """ A Monotask that writes to or reads from a single disk. """

  def __init__(self, macrotask, data_size_bytes, is_write):
    Monotask.__init__(self, macrotask)
    self.data_size_bytes = data_size_bytes
    self.is_write = is_write
    self.disk_id = None

  def __repr__(self):
    action = "write" if self.is_write else "read"
    disk = self.disk_id if self.disk_id is not None else "not assigned"
    return ("(DiskMonotask | id: %s | macrotask: %s | %s | disk: %s)" %
      (self.monotask_id, self.macrotask.macrotask_id, action, disk))

  def log_start(self, current_time_ms, worker):
    logging.info("%s: Executing %s on disk %s on %s", current_time_ms, self, self.disk_id, worker)

  def schedule(self, current_time_ms, worker):
    return worker.schedule_disk(current_time_ms, self)

  def update_worker(self, current_time_ms, worker):
    return worker.disk_monotask_end(current_time_ms, self)


class Packet(object):
  """
  Used by NetworkResponseMonotasks to send a chunk of their payload between Workers. Packets are
  queued in the source Worker's logical network output queue before being sent over the network, and
  in the destination Worker's logical network input queue before being processed.
  """

  max_size_bytes = 1500

  def __init__(self, network_response_monotask, seq_num, is_last, data_size_bytes):
    self.network_response_monotask = network_response_monotask
    # The sequence number of this Packet in its flow, starting from 0. Unique for each combination
    # of (source Worker, destination Worker, NetworkResponseMonotask). For debugging purposes only.
    self.seq_num = seq_num
    self.is_last = is_last
    self.data_size_bytes = data_size_bytes

  def __repr__(self):
    return ("(Packet | monotask: %s | src, dst: %s, %s | seq: %s | " +
      "is last: %s | data: %s bytes)") % (
        self.network_response_monotask.monotask_id,
        self.network_response_monotask.src_worker.worker_id,
        self.network_response_monotask.dst_worker.worker_id,
        self.seq_num,
        self.is_last,
        self.data_size_bytes)
