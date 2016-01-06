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

  def __init__(self, stages):
    self.job_id = Job.__new_id()
    # A deque of the Stages that make up this Job. Uses a deque instead of a list because
    # Stages in a Job are executed sequentially and once a Stage is finished we do not need to keep
    # its data structures around.
    self.waiting_stages = stages
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


class Stage(object):
  """ A Stage consists of multiple Macrotasks that are executed in parallel by many Workers. """

  __next_id = 0

  def __init__(self, macrotasks):
    self.stage_id = Stage.__new_id()
    # A list of Macrotasks that make up this Stage and are waiting to be assigned to Workers.
    self.waiting_macrotasks = macrotasks

  def __repr__(self):
    return "(Stage | id: %s)" % self.stage_id

  @staticmethod
  def __new_id():
    """ Returns a new globally-unique Stage ID. """
    old_id = Stage.__next_id
    Stage.__next_id += 1
    return old_id


class Macrotask(object):
  """ A Macrotask consists of a DAG of Monotasks that are all executed by the same Worker. """

  __next_id = 0

  def __init__(self):
    self.macrotask_id = Macrotask.__new_id()
    # A list of Monotasks that make up this Macrotask and are either currently executing or waiting
    # to be executed. Must be populated later by the code that creates this Macrotask.
    self.remaining_monotasks = []
    # Whether this Macrotask has been assigned to a Worker yet.
    self.assigned_to_worker = False

  def __repr__(self):
    return "(Macrotask | id: %s)" % self.macrotask_id

  @staticmethod
  def __new_id():
    """ Returns a new globally-unique Macrotask ID. """
    old_id = Macrotask.__next_id
    Macrotask.__next_id += 1
    return old_id

  def register_completed_monotask(self, current_time_ms, completed_monotask):
    """
    Updates this Macrotask's internal data structures to reflect that the provided Monotask has
    completed.
    """
    logging.debug(
      "%s: Removing %s from %s", current_time_ms, completed_monotask, self.remaining_monotasks)
    if completed_monotask in self.remaining_monotasks:
      self.remaining_monotasks.remove(completed_monotask)
    else:
      raise Exception("%s was removed from its Macrotask's list of Monotasks before it completed." %
        completed_monotask)

  def is_finished(self):
    """
    Returns True if all of the Monotasks in this Macrotask have completed, or False otherwise.
    """
    return len(self.remaining_monotasks) == 0


class Monotask(object):
  """
  A Monotask is a unit of work that depends on other Monotasks and uses only one system resource.
  """

  __next_id = 0

  def __init__(self, macrotask):
    self.monotask_id = Monotask.__new_id()
    # The Macrotask to which this Monotask belongs.
    self.macrotask = macrotask
    # A list of Monotasks that must complete before this Monotask can be executed. Populated by
    # add_dependency().
    self.remaining_dependencies = []
    # A list of Monotasks that cannot be executed until this Monotask completes. Populated by
    # add_dependency().
    self.dependents = []

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
    self.remaining_dependencies.append(new_dependency)
    new_dependency.dependents.append(self)

  def add_dependencies(self, new_dependencies):
    """
    Adds the provided Monotasks as dependencies of this Monotask, and this Monotask as a dependent
    of each of the provided Monotasks.
    """
    for new_dependency in new_dependencies:
      self.add_dependency(new_dependency)

  @abc.abstractmethod
  def schedule(self, current_time_ms, worker):
    """
    Passes this Monotask to the correct resource scheduler on the provided Worker. Returns a
    MonotaskEnd Event if this Monotask started executing.
    """

  @abc.abstractmethod
  def end(self, current_time_ms, worker):
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

  def end(self, current_time_ms, worker):
    return worker.compute_monotask_end(current_time_ms, self)

  def create_monotasks_for_shuffle(self, current_time_ms, local_worker, all_workers):
    """
    If this ComputeMonotask has a shuffle dependency, returns a DiskMonotask to read shuffle data
    from the local machine (if the shuffle data is stored on disk) and NetworkRequestMonotasks to
    fetch shuffle data from the remote workers.
    """
    if self.shuffle_bytes_to_read == 0:
      # This ComputeMonotask does not have a shuffle dependency.
      return []

    # The amount of shuffle data read from each Worker is equal to the total amount of shuffle data
    # required by this ComputeMonotask divided by the number of Workers.
    shuffle_bytes_per_worker = float(self.shuffle_bytes_to_read) / len(all_workers)
    new_monotasks = []
    for remote_worker in all_workers:
      if remote_worker is local_worker:
        if self.is_shuffle_data_on_disk:
          # Create a DiskMonotask to read the shuffle data from the local disk.
          new_monotask = DiskMonotask(self.macrotask, shuffle_bytes_per_worker, is_write=False)
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
          shuffle_bytes_per_worker,
          self.is_shuffle_data_on_disk)

      if new_monotask is not None:
        logging.info(
          "%s: Created %s to read shuffle data for %s", current_time_ms, new_monotask, self)
        self.add_dependency(new_monotask)
        self.macrotask.remaining_monotasks.append(new_monotask)
        new_monotasks.append(new_monotask)
    return new_monotasks


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

  def end(self, current_time_ms, worker):
    return worker.requested_data_received(self)


class NetworkResponseMonotask(Monotask):
  """
  A Monotask that transfers data from one Worker to another. Will be broken into Packets for
  transmission over the network. NetworkResponseMonotasks are not given MonotaskEnd Events. When,
  all Packets have arrived at the destination Worker, the MonotaskEnd Event for the corresponding
  NetworkRequestMonotask is created instead.
  """

  def __init__(self, macrotask, data_size_bytes, network_request_monotask):
    Monotask.__init__(self, macrotask)
    self.data_size_bytes = data_size_bytes
    self.network_request_monotask = network_request_monotask
    self.src_worker = network_request_monotask.dst_worker
    self.dst_worker = network_request_monotask.src_worker

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

  def end(self, current_time_ms, worker):
    raise NotImplementedError("NetworkResponseMonotasks do not support end(). When a " +
      "NetworkResponseMonotask has completed, create a MonotaskEnd Event for the associated " +
      "NetworkRequestMonotask instead.")

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

  def end(self, current_time_ms, worker):
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
