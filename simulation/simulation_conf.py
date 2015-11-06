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
from xml.dom import minidom

import task_constructs


class SimulationConf(object):
  """Contains Simulator setup parameters and Job information.

  Extracts simulation parameters from an XML configuration file. Creates a deque of Jobs for a
  Simulator to execute.
  """

  def __init__(self, conf_file_path):
    dom = minidom.parse(conf_file_path)
    simulator_dom = dom.getElementsByTagName("simulator")[0]

    self.num_workers = self.__parse_int(simulator_dom, "num_workers")
    self.num_cores = self.__parse_int(simulator_dom, "num_cores_per_worker")
    self.network_bandwidth_Mbps = self.__parse_float(simulator_dom, "worker_network_bandwidth_Mbps")
    self.network_latency_ms = self.__parse_float(simulator_dom, "worker_network_latency_ms")

    # Maps disk id to a tuple of (write throughput MB/s, read throughput MB/s)
    self.disks = self.__parse_disks(simulator_dom.getElementsByTagName("disks_per_worker"))
    self.jobs = self.__parse_jobs(simulator_dom.getElementsByTagName("jobs")[0], self.disks.keys())

  @staticmethod
  def __parse_disks(disks_dom):
    """
    Parses the Worker disk configuration parameters from the provided DOM. Returns a dictionary
    mapping disk id to a tuple of (write throughput MB/s, read throughput MB/s).
    """
    disks = {}
    if len(disks_dom) == 0:
      logging.debug("No disks specified.")
      return disks

    for disk_dom in disks_dom[0].getElementsByTagName("disk"):
      disk_id = SimulationConf.__parse_string(disk_dom, "id")
      write_throughput_MBps = SimulationConf.__parse_float(disk_dom, "write_throughput_MBps")
      read_throughput_MBps = SimulationConf.__parse_float(disk_dom, "read_throughput_MBps")
      disks[disk_id] = (write_throughput_MBps, read_throughput_MBps)
    return disks

  @staticmethod
  def __parse_jobs(jobs_dom, disk_ids):
    """ Returns a deque of Job objects parsed from the provided DOM. """
    jobs = collections.deque()
    for job_dom in jobs_dom.getElementsByTagName("job"):
      stages = SimulationConf.__parse_stages(job_dom.getElementsByTagName("stages")[0], disk_ids)
      jobs.append(task_constructs.Job(stages))
    logging.info("Found %s Job(s)", len(jobs))
    return jobs

  @staticmethod
  def __parse_stages(stages_dom, disk_ids):
    """ Returns a deque of Stage objects parsed from the provided DOM. """
    stages = collections.deque()
    for stage_dom in stages_dom.getElementsByTagName("stage"):
      num_partitions = SimulationConf.__parse_int(stage_dom, "num_partitions")
      monotasks_dom = stage_dom.getElementsByTagName("monotasks_per_partition")[0]
      macrotasks = SimulationConf.__parse_macrotasks(monotasks_dom, num_partitions, disk_ids)
      stages.append(task_constructs.Stage(macrotasks))
    return stages

  @staticmethod
  def __parse_macrotasks(monotasks_dom, num_partitions, disk_ids):
    """ Returns num_partitions Macrotasks created using the template in monotasks_dom. """
    monotask_doms = monotasks_dom.getElementsByTagName("monotask")
    macrotasks = []
    for _ in xrange(num_partitions):
      macrotask = task_constructs.Macrotask()
      dag_id_to_monotask = {}
      monotask_to_dependency_dag_ids = {}

      # Reparse all of the monotasks for each Macrotask so that if the Macrotask contains disk
      # read monotasks, they are not all configured to read from the same disks.
      for monotask_dom in monotask_doms:
        monotask = SimulationConf.__parse_monotask(
          monotask_dom, macrotask, num_partitions, disk_ids)
        # The string dag_id specified in the configuration file is independent of the numeric
        # monotask_id defined in the Monotask class because monotask_id must be unique for all
        # Monotasks, whereas dag_id is only unique within a Stage (for ease of configuration).
        dag_id_to_monotask[SimulationConf.__parse_string(monotask_dom, "dag_id")] = monotask

        dependency_dag_ids_doms = monotask_dom.getElementsByTagName("dependency_dag_ids")
        dependency_dag_ids = []
        if len(dependency_dag_ids_doms) != 0:
          dependency_elements = dependency_dag_ids_doms[0].getElementsByTagName("dependency_dag_id")
          dependency_dag_ids = [dependency_dag_id_element.firstChild.data
            for dependency_dag_id_element in dependency_elements]
        monotask_to_dependency_dag_ids[monotask] = dependency_dag_ids

      # Now that we have created all of the monotasks objects, we hook up their dependencies and add
      # them to the Macrotask.
      for monotask, dependency_dag_ids in monotask_to_dependency_dag_ids.iteritems():
        dependencies = [dag_id_to_monotask[dependency_dag_id]
          for dependency_dag_id in dependency_dag_ids]
        logging.info("Adding dependencies to %s: %s", monotask, dependencies)
        monotask.add_dependencies(dependencies)

        macrotask.remaining_monotasks.append(monotask)
      macrotasks.append(macrotask)
    return macrotasks

  @staticmethod
  def __parse_monotask(monotask_dom, macrotask, num_partitions, disk_ids):
    """
    Returns a Monotask object parsed from the provided DOM that has been initialized to be part of
    the specified Macrotask. Does not set up the Monotask's dependencies.
    """
    monotask_type = SimulationConf.__parse_string(monotask_dom, "type")
    if monotask_type == "compute":
      compute_time_ms = SimulationConf.__parse_float(monotask_dom, "compute_time_ms")

      # NetworkMonotasks are specified implicitly by defining a shuffle dependency. We cannot
      # explicitly specify NetworkMonotasks because we do not know where the shuffle data is
      # located.
      total_shuffle_bytes, is_shuffle_data_on_disk = (
        SimulationConf.__parse_shuffle_dependency_info(monotask_dom))

      # The amount of shuffle data read by each reduce task is equal to the total amount of shuffle
      # data divided by the number of reduce tasks.
      shuffle_bytes_to_read = float(total_shuffle_bytes) / num_partitions

      return task_constructs.ComputeMonotask(
        macrotask,
        compute_time_ms,
        shuffle_bytes_to_read,
        is_shuffle_data_on_disk,
        num_partitions)
    elif monotask_type == "disk":
      is_write = SimulationConf.__parse_bool(monotask_dom, "is_write")
      data_size_bytes = SimulationConf.__parse_int(monotask_dom, "data_size_bytes")
      disk_monotask = task_constructs.DiskMonotask(macrotask, data_size_bytes, is_write)

      if not is_write:
        # Assume the data to be read is evenly distributed across disks. Select a disk id in round
        # robin order.
        disk_monotask.disk_id = disk_ids[disk_monotask.monotask_id % len(disk_ids)]
      return disk_monotask
    else:
      raise Exception("Unknown monotask type: %s" % monotask_type)

  @staticmethod
  def __parse_shuffle_dependency_info(monotask_dom):
    """
    Returns a tuple of (total shuffle data size bytes, is on disk?) extracted from the shuffle
    dependency info in the provided Monotask DOM, or (0, False) if the Monotask does not have a
    shuffle dependency.
    """
    shuffle_dependency_elements = monotask_dom.getElementsByTagName("shuffle_dependency")
    if len(shuffle_dependency_elements) != 1:
      return (0, False)

    shuffle_dependency_element = shuffle_dependency_elements[0]
    total_size_bytes = SimulationConf.__parse_long(shuffle_dependency_element, "total_size_bytes")
    is_on_disk = SimulationConf.__parse_bool(shuffle_dependency_element, "is_on_disk")
    return (total_size_bytes, is_on_disk)

  @staticmethod
  def __parse_bool(dom, tag):
    """ Extracts the boolean corresponding to the given tag in the provided DOM. """
    return SimulationConf.__parse_string(dom, tag) == "True"

  @staticmethod
  def __parse_float(dom, tag):
    """ Extracts the float corresponding to the given tag in the provided DOM. """
    return float(SimulationConf.__parse_string(dom, tag))

  @staticmethod
  def __parse_int(dom, tag):
    """ Extracts the int corresponding to the given tag in the provided DOM. """
    return int(SimulationConf.__parse_string(dom, tag))

  @staticmethod
  def __parse_long(dom, tag):
    """ Extracts the long corresponding to the given tag in the provided DOM. """
    return long(SimulationConf.__parse_string(dom, tag))

  @staticmethod
  def __parse_string(dom, tag):
    """ Extracts the string corresponding to the given tag in the provided DOM. """
    try:
      return dom.getElementsByTagName(tag)[0].firstChild.data
    except IndexError:
      raise Exception("No element with tag: %s" % tag)

  def get_network_bandwidth_Bpms(self):
    """ Returns the network bandwidth in B/ms. """
    return float(self.network_bandwidth_Mbps * 1000) / 8

  def get_throughput_Bpms_for_disk(self, disk_id, is_write):
    """
    If is_write is True, returns the write throughput of the specified disk, otherwise returns the
    disk's read throughput. Return values are in B/ms.
    """
    write_throughput, read_throughput = self.disks[disk_id]
    throughput_MBps = write_throughput if is_write else read_throughput
    return throughput_MBps * 1000
