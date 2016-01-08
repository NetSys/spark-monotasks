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


class Event(object):
  """ Abstract class representing simulation Events. """

  def __cmp__(self, other):
    my_value = str(self)
    other_value = str(other)

    if my_value < other_value:
      return -1
    elif my_value == other_value:
      return 0
    else:
      return 1

  @abc.abstractmethod
  def run(self, current_time_ms):
    """
    Executes this Event's logic and returns a list of tuples of the form (Event time ms, Event) for
    any Events that should be added to the Event queue.
    """


class JobStart(Event):
  """ Event that notifies the simulated Monotasks master node to start a new Job. """

  def __init__(self, simulator, job):
    self.simulator = simulator
    self.job = job

  def __repr__(self):
    return "JobStart Event for %s" % self.job

  def run(self, current_time_ms):
    return self.simulator.start_job(current_time_ms, self.job)


class MacrotaskRequest(Event):
  """
  Event that notifies the Monotasks master node that a Worker requires an additional Macrotask.
  """

  def __init__(self, worker):
    self.worker = worker

  def __repr__(self):
    return "MacrotaskRequest Event from %s" % self.worker

  def run(self, current_time_ms):
    return self.worker.simulator.send_macrotask_to_worker(current_time_ms, self.worker)


class NotifyMasterOfMacrotaskEnd(Event):
  """
  Event that notifies the simulated Monotasks master node that a Macrotask has completed. This logic
  cannot be a part of the MonotaskEnd Event because there is a network delay between when a Worker
  finishes executing the last Monotask in a Macrotask and when the master node is told that the
  Macrotask has completed.
  """

  def __init__(self, macrotask):
    self.macrotask = macrotask

  def __repr__(self):
    return "NotifyMasterOfMacrotaskEnd Event for %s" % self.macrotask

  def run(self, current_time_ms):
    logging.info("%s: Macrotask completed: %s", current_time_ms, self.macrotask)
    return self.macrotask.worker.simulator.finish_macrotask(current_time_ms, self.macrotask)


class MacrotaskStart(Event):
  """ Event that notifies a Worker that a Macrotask has arrived. """

  def __init__(self, macrotask):
    self.macrotask = macrotask

  def __repr__(self):
    return "MacrotaskStart Event for %s on %s" % (self.macrotask, self.macrotask.worker)

  def run(self, current_time_ms):
    logging.info("%s: %s starting on %s", current_time_ms, self.macrotask, self.macrotask.worker)
    return self.macrotask.worker.handle_macrotask_start(current_time_ms, self.macrotask)


class MonotaskEnd(Event):
  """ Event that notifies a Worker that a Monotask has completed. """

  def __init__(self, worker, monotask):
    self.worker = worker
    self.monotask = monotask

  def __repr__(self):
    return "MonotaskEnd Event for %s" % self.monotask

  def run(self, current_time_ms):
    logging.info("%s: Monotask completed: %s", current_time_ms, self.monotask)
    # Get Events created by freeing resources.
    new_events = self.monotask.end(current_time_ms, self.worker)
    new_events.extend(self.worker.handle_finished_monotask(current_time_ms, self.monotask))
    return new_events


class NetworkRequest(Event):
  """ Event that notifies a Worker that a remote Worker is requesting data. """

  def __init__(self, network_request_monotask):
    self.network_request_monotask = network_request_monotask

  def __repr__(self):
    return "NetworkRequest Event for %s" % self.network_request_monotask

  def run(self, current_time_ms):
    return self.network_request_monotask.dst_worker.handle_network_request(
      current_time_ms, self.network_request_monotask)


class PacketDeparture(Event):
  """
  Event that notifies a Worker that a Packet has been completely transmitted on an outgoing link.
  Required so that the sending Worker can accurately keep track of the total number of bytes that it
  has sent.
  """

  def __init__(self, packet):
    self.packet = packet

  def __repr__(self):
    return "PacketDeparture Event for %s" % self.packet

  def run(self, current_time_ms):
    return self.packet.network_response_monotask.src_worker.handle_packet_departure(
      current_time_ms, self.packet)


class PacketArrival(Event):
  """
  Event that notifies a Worker that a Packet has completely arrived and is ready to be processed.
  Required so that the receiving Worker can accurately keep track of the total number of bytes that
  it has received.
  """

  def __init__(self, packet):
    self.packet = packet

  def __repr__(self):
    return "PacketArrival Event for %s" % self.packet

  def run(self, current_time_ms):
    return self.packet.network_response_monotask.dst_worker.handle_packet_arrival(
      current_time_ms, self.packet)


class LogContinuousMonitors(Event):
  """
  Event that creates a ContinuousMonitor log entry for each Worker. Only one object of this class is
  created, and it is rescheduled every self.log_interval_ms.
  """

  def __init__(self, workers, log_interval_ms):
    self.workers = workers
    self.log_interval_ms = log_interval_ms

  def __repr__(self):
    return "LogContinuousMonitors Event"

  def run(self, current_time_ms):
    """
    Creates a ContinuousMonitor log entry for each worker, then returns this Event to be rescheduled
    for self.log_interval_ms in the future.
    """
    for worker in self.workers:
      worker.continuous_monitor.log(current_time_ms)
    return [(current_time_ms + self.log_interval_ms, self)]
