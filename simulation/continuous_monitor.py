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

import json


class ContinuousMonitor(object):
  """ Logs the state of a single Worker. """

  __empty_cpu_counters = {
    "Time Milliseconds": 0,
    "Process User Jiffies": 0,
    "Process System Jiffies": 0,
    "Total User Jiffies": 0,
    "Total System Jiffies": 0
  }

  def __init__(
      self,
      log_path,
      get_outstanding_network_bytes,
      get_total_bytes_sent,
      get_total_bytes_received,
      get_num_running_compute_monotasks,
      get_disk_id_to_num_monotasks,
      get_disk_id_to_util,
      get_num_running_macrotasks,
      get_num_macrotasks_in_compute,
      get_num_macrotasks_in_network,
      get_num_macrotasks_in_disk):
    self.log_file = open(log_path, "w")
    self.is_open = True

    self.previous_time_ms = 0
    self.previous_total_bytes_sent = 0
    self.previous_total_bytes_received = 0
    self.previous_bytes_sent_per_s = 0
    self.previous_bytes_received_per_s = 0

    self.get_outstanding_network_bytes = get_outstanding_network_bytes
    self.get_total_bytes_sent = get_total_bytes_sent
    self.get_total_bytes_received = get_total_bytes_received
    self.get_num_running_compute_monotasks = get_num_running_compute_monotasks
    self.get_disk_id_to_num_monotasks = get_disk_id_to_num_monotasks
    self.get_disk_id_to_util = get_disk_id_to_util
    self.get_num_running_macrotasks = get_num_running_macrotasks
    self.get_num_macrotasks_in_compute = get_num_macrotasks_in_compute
    self.get_num_macrotasks_in_network = get_num_macrotasks_in_network
    self.get_num_macrotasks_in_disk = get_num_macrotasks_in_disk

  def log(self, current_time_ms):
    """ Writes a JSON log entry describing the current state of the Worker. """
    if self.is_open:
      self.log_file.write(json.dumps(self.generate_log(current_time_ms)))
      self.log_file.write("\n")
    else:
      raise Exception("ContinuousMonitor is closed.")

  def generate_log(self, current_time_ms):
    """ Returns a dictionary representation of a continuous monitor entry. """
    elapsed_time_ms = current_time_ms - self.previous_time_ms
    log = {
      "Current Time": current_time_ms,
      "Previous Time": self.previous_time_ms,
      "Fraction GC Time": 0,
      "Cpu Utilization": self.build_cpu_utilization(),
      "Disk Utilization": self.build_disk_utilization(elapsed_time_ms),
      "Network Utilization": self.build_network_utilization(elapsed_time_ms),
       "Outstanding Network Bytes": self.get_outstanding_network_bytes(),
      "Running Compute Monotasks": self.get_num_running_compute_monotasks(),
      "Running Disk Monotasks": self.build_running_disk_monotasks(),
      "Running Macrotasks": self.get_num_running_macrotasks(),
      "Macrotasks In Compute": self.get_num_macrotasks_in_compute(),
      "Macrotasks In Disk": self.get_num_macrotasks_in_disk(),
      "Macrotasks In Network": self.get_num_macrotasks_in_network(),
      "Free Heap Memory Bytes": 0,
      "Free Off-Heap Memory Bytes": 0
    }
    self.previous_time_ms = current_time_ms
    return log

  def build_cpu_utilization(self):
    num_cores_in_use = self.get_num_running_compute_monotasks()
    return {
      "Start Counters": ContinuousMonitor.__empty_cpu_counters,
      "End Counters": ContinuousMonitor.__empty_cpu_counters,
      "Process User Utilization": num_cores_in_use,
      "Process System Utilization": 0,
      "Total User Utilization": num_cores_in_use,
      "Total System Utilization": 0
    }

  def build_network_utilization(self, elapsed_time_ms):
    new_total_bytes_sent = self.get_total_bytes_sent()
    new_total_bytes_received = self.get_total_bytes_received()
    if elapsed_time_ms > 0:
      elapsed_time_s = float(elapsed_time_ms) / 1000
      self.previous_bytes_sent_per_s = (
        float(new_total_bytes_sent - self.previous_total_bytes_sent) / elapsed_time_s)
      self.previous_bytes_received_per_s = (
        float(new_total_bytes_received - self.previous_total_bytes_received) / elapsed_time_s)

    network_util = {
      "Elapsed Millis": elapsed_time_ms,
      "Bytes Received Per Second": self.previous_bytes_received_per_s,
      "Bytes Transmitted Per Second": self.previous_bytes_sent_per_s,
      "Packets Received Per Second": 0,
      "Packets Transmitted Per Second": 0
    }
    self.previous_total_bytes_sent = new_total_bytes_sent
    self.previous_total_bytes_received = new_total_bytes_received
    return network_util

  def build_disk_utilization(self, elapsed_time_ms):
    return {
      "Elapsed Millis": elapsed_time_ms,
      "Device Name To Utilization": self.get_disk_id_to_util()
    }

  def build_running_disk_monotasks(self):
    return [{
      "Disk Name": disk_id,
      "Running And Queued Monotasks": num_monotasks
    } for disk_id, num_monotasks in self.get_disk_id_to_num_monotasks().iteritems()]

  def close(self):
    """ Closes this ContinuousMonitor. Any additional calls to log() will fail. """
    if self.is_open:
      self.is_open = False
      self.log_file.close()
