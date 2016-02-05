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

import collections
import logging
from os import path
import random
import sets

import continuous_monitor
import events
import scheduler
import simulation_conf
import task_constructs


class Worker(object):
  """
  Models a single worker node that executes Macrotasks and Monotasks. Access to each resource (CPU,
  network, disk) is controlled separately to prevent contention.

  The vast majority of simulation logic lives here. Event and task classes use Worker methods to
  update state so that they are not exposed to the inner workings of this class.
  """

  __next_id = 0

  def __init__(self, simulator, conf, continuous_monitor_dir):
    self.worker_id = Worker.__new_id()
    self.simulator = simulator
    self.conf = conf

    self.running_compute_monotasks = []
    # A FIFO queue of waiting ComputeMonotasks.
    self.compute_queue = collections.deque()

    # A list of in-progess NetworkRequestMonotasks.
    self.outstanding_network_requests = []
    self.network_output_queue_end_ms = 0
    self.network_input_queue_end_ms = 0
    self.total_bytes_sent = 0
    self.total_bytes_received = 0

    # Maps disk id to a queue of DiskMonotasks. The first DiskMonotask in each queue is currently
    # executing.
    self.disks = {disk_id: collections.deque() for disk_id in self.conf.disks.iterkeys()}
    # The index of the next disk id to be assigned to a disk write Monotask.
    self.next_disk_id_index = 0

    # Used by the Simulator to keep track of how many Macrotasks have been assigned to this Worker.
    # This many be larger than the number of currently running Macrotasks if a Macrotask that has
    # been assigned either has not arrived at the Worker yet or has just completed.
    self.num_assigned_macrotasks = 0
    self.scheduler = scheduler.Scheduler.get_scheduler_for_mode(self.conf.scheduling_mode, self)
    # Monotasks that cannot execute because their dependencies have not finished yet.
    self.not_runnable_monotasks = []

    self.continuous_monitor = continuous_monitor.ContinuousMonitor(
      path.join(continuous_monitor_dir, "%s_continuous_monitor" % self.worker_id),
      self.get_outstanding_network_bytes,
      self.get_total_bytes_sent,
      self.get_total_bytes_received,
      self.get_num_running_compute_monotasks,
      self.get_disk_id_to_num_monotasks,
      self.get_disk_id_to_util,
      self.get_num_running_macrotasks,
      self.get_num_macrotasks_in_compute,
      self.get_num_macrotasks_in_network,
      self.get_num_macrotasks_in_disk)

    self.__log_params()

  def __repr__(self):
    return "(Worker | id: %s)" % self.worker_id

  @staticmethod
  def __new_id():
    """ Returns a new globally-unique Worker ID. """
    old_id = Worker.__next_id
    Worker.__next_id += 1
    return old_id

  def __get_next_disk_id(self):
    """ Returns the next disk id selected in round robin order. """
    sorted_disk_ids = sorted(self.disks.keys())
    disk_id = sorted_disk_ids[self.next_disk_id_index]
    self.next_disk_id_index = (self.next_disk_id_index + 1) % len(self.disks)
    return disk_id

  def __log_params(self):
    """ Logs the parameters used to configure this Worker. """
    logging.info(
      "Created Worker with %s cores and disks: [%s] using scheduling mode: %s",
      self.conf.num_cores,
      simulation_conf.SimulationConf.format_disk_info(self.conf.disks, separator=", "),
      self.conf.scheduling_mode)

  def handle_macrotask_start(self, current_time_ms, macrotask):
    """Starts executing the provided Macrotask on this Worker.

    Creates any Monotasks required to read shuffle data, informs this Worker's Scheduler that the
    Macrotask has arrived, and submits the Macrotask's Monotasks for execution.

    Returns:
      MonotaskEnd Events for any Monotasks that started executing.
    """
    # Create any Monotasks necessary for reading shuffle data before telling the Scheduler about the
    # Macrotask so that it sees all of the Monotasks.
    for monotask in macrotask.monotasks:
      if isinstance(monotask, task_constructs.ComputeMonotask):
        # Since this is a ComputeMonotask, it might have a shuffle dependency.
        monotask.create_monotasks_for_shuffle(current_time_ms, self.simulator.workers)

    self.scheduler.handle_macrotask_start(macrotask)
    return self.submit_monotasks(current_time_ms, macrotask.monotasks)

  def submit_monotasks(self, current_time_ms, monotasks_to_submit):
    """
    Submits the provided Monotasks for scheduling, returning a list of MonotaskEnd Events for any
    Monotasks that were able to start executing.
    """
    new_events = []
    for monotask in monotasks_to_submit:
      logging.debug(
        "%s: Submitting %s with dependency ids: %s",
        current_time_ms,
        monotask,
        [dependency.monotask_id for dependency in monotask.dependencies])
      if monotask.dependencies_have_finished():
        new_events.extend(monotask.schedule(current_time_ms, self))
      else:
        self.not_runnable_monotasks.append(monotask)
    return new_events

  #-----------------------------------------#
  # Monotask start logic                    #
  #-----------------------------------------#

  def schedule_compute(self, current_time_ms, compute_monotask):
    """
    Adds the provided ComputeMonotask to the compute queue, returning a MonotaskEnd Event if it
    started executing.
    """
    self.compute_queue.append(compute_monotask)
    return self.__service_compute_queue(current_time_ms)

  def __service_compute_queue(self, current_time_ms):
    """
    Start executing a ComputeMonotask from the compute queue if the queue is not empty and fewer
    than the maximum number of ComputeMonotask are currently executing. Returns a MonotaskEnd Event
    if a ComputeMonotask started executing.
    """
    if ((len(self.compute_queue) != 0) and
        (len(self.running_compute_monotasks) < self.conf.num_cores)):
      monotask_to_execute = self.compute_queue.popleft()
      monotask_to_execute.log_start(current_time_ms, self)
      self.running_compute_monotasks.append(monotask_to_execute)
      monotask_end_time_ms = current_time_ms + monotask_to_execute.compute_time_ms
      return [(monotask_end_time_ms, events.MonotaskEnd(self, monotask_to_execute))]
    else:
      return []

  def schedule_network_request(self, current_time_ms, network_request_monotask):
    """
    Creates a remote NetworkRequest Event for the specified NetworkRequestMonotask. The
    corresponding MonotaskEnd Event is created when the last Packet of the corresponding
    NetworkResponseMonotask arrives.
    """
    self.outstanding_network_requests.append(network_request_monotask)
    request_arrival_time_ms = current_time_ms + self.conf.network_latency_ms
    return [(request_arrival_time_ms, events.NetworkRequest(network_request_monotask))]

  def handle_network_request(self, current_time_ms, network_request_monotask):
    """
    Updates this Worker's metadata to reflect that it received the specified
    NetworkRequestMonotask. Creates and submits the Monotasks required to read the data and send it
    back to the requesting Worker, returning any new Events.
    """
    macrotask = network_request_monotask.macrotask
    request_size_bytes = network_request_monotask.request_size_bytes
    network_response_monotask = task_constructs.NetworkResponseMonotask(
      macrotask, request_size_bytes, network_request_monotask)
    new_monotasks = [network_response_monotask]

    if network_request_monotask.is_on_disk:
      # If the shuffle data is stored on disk, then we need to create a DiskMonotask to read it.
      # TODO: We need a way to track which disk the block is on. For now, we just pick a disk
      #       uniformly at random.
      disk_read_monotask = task_constructs.DiskMonotask(
        macrotask, request_size_bytes, is_write=False)
      disk_read_monotask.disk_id = random.choice(self.disks.keys())
      logging.info(
        "%s: Created %s to read shuffle data requested by %s",
        current_time_ms,
        disk_read_monotask,
        network_request_monotask)

      network_response_monotask.add_dependency(disk_read_monotask)
      new_monotasks.append(disk_read_monotask)

    return self.submit_monotasks(current_time_ms, new_monotasks)

  def schedule_network_response(self, current_time_ms, network_response_monotask):
    """
    Breaks the provided NetworkResponseMonotask into Packets, adds them to the network output
    queue, and returns the corresponding PacketDeparture Events.
    """
    transmit_start_ms = max(current_time_ms, self.network_output_queue_end_ms)
    network_bandwidth_Bpms = network_response_monotask.network_bandwidth_Bpms
    new_events = []

    for packet in network_response_monotask.get_packets(current_time_ms):
      transmit_end_ms = transmit_start_ms + (float(packet.data_size_bytes) / network_bandwidth_Bpms)

      new_events.append((transmit_end_ms, events.PacketDeparture(packet)))
      transmit_start_ms = transmit_end_ms

    self.network_output_queue_end_ms = transmit_end_ms
    return new_events

  def handle_packet_departure(self, current_time_ms, packet):
    """
    Updates this Worker's metadata to reflect that the specified Packet has been completely
    transmitted. Returns a PacketArrival Event for when the Packet will arrive at its destination
    Worker. If this is the last Packet in a NetworkResponseMonotask, then this method also returns a
    MonotaskEnd Event for the NetworkResponseMonotask.
    """
    logging.debug("%s: %s completely sent by %s", current_time_ms, packet, self)
    packet_size_bytes = packet.data_size_bytes
    self.total_bytes_sent += packet_size_bytes

    dst_worker = packet.network_response_monotask.network_request_monotask.dst_worker
    transmit_start_ms = max(
      current_time_ms + self.conf.network_latency_ms, dst_worker.network_input_queue_end_ms)
    transmit_end_ms = transmit_start_ms + (
      float(packet_size_bytes) / packet.network_response_monotask.network_bandwidth_Bpms)
    dst_worker.network_input_queue_end_ms = transmit_end_ms
    new_events = [(transmit_end_ms, events.PacketArrival(packet))]

    if packet.is_last:
      new_events.append(
        (current_time_ms, events.MonotaskEnd(self, packet.network_response_monotask)))
    return new_events

  def schedule_disk(self, current_time_ms, disk_monotask):
    """
    Adds the provided DiskMonotask to the appropriate disk queue, returning a MonotaskEnd Event if
    it started executing.
    """
    if disk_monotask.disk_id is None:
      # Disk write Monotasks have not been assigned a disk id yet. Pick one in round robin order.
      disk_monotask.disk_id = self.__get_next_disk_id()

    disk_id = disk_monotask.disk_id
    if disk_id not in self.disks:
      raise Exception("Disk id %s for %s not in known disk ids: %s" %
        (disk_id, disk_monotask, self.disks.keys()))

    logging.debug(
      "%s: Adding %s to the queue for disk %s on %s", current_time_ms, disk_monotask, disk_id, self)
    disk_queue = self.disks[disk_id]
    disk_queue.append(disk_monotask)

    if len(disk_queue) == 1:
      # Start executing the provided DiskMonotask if it immediately went to the head of the queue.
      disk_monotask.log_start(current_time_ms, self)
      return [self.__get_disk_monotask_end_event(current_time_ms, disk_monotask, disk_id)]
    else:
      return []

  def __get_disk_monotask_end_event(self, current_time_ms, disk_monotask, disk_id):
    """
    Creates a MonotaskEnd event for when the provided DiskMonotask will be finished accessing its
    disk. Assumes that its disk is currently idle.
    """
    disk_throughput_Bpms = self.conf.get_throughput_Bpms_for_disk(disk_id, disk_monotask.is_write)
    disk_time_ms = float(disk_monotask.data_size_bytes) / disk_throughput_Bpms
    return (current_time_ms + disk_time_ms, events.MonotaskEnd(self, disk_monotask))

  #-----------------------------------------#
  # Monotask end logic                      #
  #-----------------------------------------#

  def compute_monotask_end(self, current_time_ms, completed_compute_monotask):
    """
    Updates this Worker's compute usage data structures to reflect that the provided ComputeMonotask
    finished executing. Returns a MonotaskEnd Event if another waiting ComputeMonotask started
    executing. This method is not responsible for updating the Monotask dependency graph.
    """
    if completed_compute_monotask in self.running_compute_monotasks:
      self.running_compute_monotasks.remove(completed_compute_monotask)
    else:
      raise Exception("Unknown ComputeMonotask completed: %s" % completed_compute_monotask)
    return self.__service_compute_queue(current_time_ms)

  def handle_packet_arrival(self, current_time_ms, packet):
    """
    Updates this Worker's metadata to reflect that the specified Packet has been completely
    received and is ready to be processed. Returns a MonotaskEnd Event for the corresponding
    NetworkRequestMonotask if this is the last Packet in a NetworkResponseMonotask.
    """
    logging.debug("%s: %s completely received by %s", current_time_ms, packet, self)
    self.total_bytes_received += packet.data_size_bytes

    if packet.is_last:
      network_request_monotask = packet.network_response_monotask.network_request_monotask
      return [(current_time_ms, events.MonotaskEnd(self, network_request_monotask))]
    else:
      return []

  def requested_data_received(self, completed_network_request_monotask):
    """
    Records that the data requested by the provided NetworkRequestMonotask has been received,
    meaning that the Monotask has completed. Does not return a new MonotaskEnd Event because there
    is no queueing of NetworkRequestMonotasks. This method is not responsible for updating the
    Monotask dependency graph.
    """
    if completed_network_request_monotask in self.outstanding_network_requests:
      self.outstanding_network_requests.remove(completed_network_request_monotask)
    else:
      raise Exception("%s not found in list of outstanding network requests: %s" %
        (completed_network_request_monotask, self.outstanding_network_requests))
    return []

  def disk_monotask_end(self, current_time_ms, completed_disk_monotask):
    """
    Updates this Worker's disk usage data structures to reflect that the provided DiskMonotask
    finished executing. Returns a MonotaskEnd Event if another waiting DiskMonotask started
    executing. This method is not responsible for updating the Monotask dependency graph.
    """
    disk_id = completed_disk_monotask.disk_id
    if disk_id not in self.disks:
      raise Exception("Disk id %s for %s not in known disk ids: %s" %
        (disk_id, completed_disk_monotask, self.disks.keys()))

    disk_queue = self.disks[disk_id]
    if completed_disk_monotask != disk_queue.popleft():
      raise Exception("Attempting to complete %s when it is not at the head of disk queue %s" %
                      (completed_disk_monotask, disk_queue))

    if len(disk_queue) > 0:
      disk_monotask_to_execute = disk_queue[0]
      disk_monotask_to_execute.log_start(current_time_ms, self)
      return [
        self.__get_disk_monotask_end_event(current_time_ms, disk_monotask_to_execute, disk_id)]
    else:
      return []

  def handle_finished_monotask(self, current_time_ms, completed_monotask):
    """
    Updates the Macrotask and its Scheduler to reflect that the provided Monotask has completed.
    Possibly returns MonotaskEnd Events, a NotifyMasterOfMacrotaskEnd Event, and/or a
    MacrotaskRequest Event.
    """
    new_events = self.__update_dag_for_finished_monotask(current_time_ms, completed_monotask)
    new_events.extend(self.scheduler.handle_monotask_end(current_time_ms, completed_monotask))
    return new_events

  def __update_dag_for_finished_monotask(self, current_time_ms, completed_monotask):
    """
    Submits for scheduling any of the newly finished Monotask's dependents that are now eligible to
    be executed. Returns MonotaskEnd Events for any dependents that started executing. Returns a
    NotifyMasterOfMacrotaskEnd Event if all of the Monotasks for the Macrotask have completed.
    """
    macrotask = completed_monotask.macrotask
    if macrotask.all_monotasks_finished():
      arrival_time_ms = current_time_ms + self.conf.network_latency_ms
      return [(arrival_time_ms, events.NotifyMasterOfMacrotaskEnd(macrotask))]
    else:
      # If the Macrotask has not finished, then the completed Monotask may have dependents that need
      # to be updated to reflect that it finished.
      new_events = []
      for dependent in completed_monotask.dependents:
        if dependent.dependencies_have_finished():
          self.not_runnable_monotasks.remove(dependent)
          new_events.extend(dependent.schedule(current_time_ms, self))

      return new_events

  #-----------------------------------------#
  # Instrumentation                         #
  #-----------------------------------------#

  def get_outstanding_network_bytes(self):
    return sum([monotask.request_size_bytes for monotask in self.outstanding_network_requests])

  def get_total_bytes_sent(self):
    return self.total_bytes_sent

  def get_total_bytes_received(self):
    return self.total_bytes_received

  def get_num_running_compute_monotasks(self):
    return len(self.running_compute_monotasks)

  def get_disk_id_to_num_monotasks(self):
    return {disk_id: len(queue) for disk_id, queue in self.disks.iteritems()}

  def get_disk_id_to_util(self):
    disk_utils = []
    for disk_id, disk_queue in self.disks.iteritems():
      disk_util = {
        "Disk Utilization": 0,
        "Read Throughput": 0,
        "Write Throughput": 0
      }

      if len(disk_queue) > 0:
        disk_util["Disk Utilization"] = 1
        is_write = disk_queue[0].is_write
        key = "Write Throughput" if is_write else "Read Throughput"
        disk_util[key] = self.conf.get_throughput_Bpms_for_disk(disk_id, is_write) * 1000

      disk_utils.append({disk_id: disk_util})
    return disk_utils

  def get_num_running_macrotasks(self):
    return (self.get_num_macrotasks_in_compute() + self.get_num_macrotasks_in_network() +
      self.get_num_macrotasks_in_disk())

  def get_num_macrotasks_in_compute(self):
    macrotask_ids = sets.Set()
    for monotask in self.running_compute_monotasks:
      macrotask_ids.add(monotask.macrotask.macrotask_id)
    for monotask in self.compute_queue:
      macrotask_ids.add(monotask.macrotask.macrotask_id)
    return len(macrotask_ids)

  def get_num_macrotasks_in_network(self):
    macrotask_ids = sets.Set()
    for monotask in self.outstanding_network_requests:
      macrotask_ids.add(monotask.macrotask.macrotask_id)
    return len(macrotask_ids)

  def get_num_macrotasks_in_disk(self):
    macrotask_ids = sets.Set()
    for queue in self.disks.itervalues():
      macrotask_ids.update([queued_monotask.macrotask.macrotask_id for queued_monotask in queue])
    return len(macrotask_ids)
