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

import logging
import random
from xml.dom import minidom

import task_constructs


class SimulationConf(object):
  """Contains Simulator setup parameters and Job information.

  Instance variables are set manually by the caller.
  """

  def __init__(self):
    self.num_workers = 0
    self.scheduling_mode = ""
    self.throttling_scheduler_macrotask_buffer_size = 0
    self.num_cores = 0
    self.network_bandwidth_Bpms = 0.0
    self.network_bandwidth_variance = 0.0
    self.network_latency_ms = 0.0
    # Maps disk id to a tuple of (write throughput B/ms, read throughput B/ms)
    self.disks = {}
    self.jobs = []

  def __repr__(self):
    disk_description = SimulationConf.format_disk_info(self.disks, separator="\n    ")
    return ("SimulationConf with parameters:\n" +
      ("  number of workers: %s\n" % self.num_workers) +
      ("  scheduling mode: %s\n" % self.scheduling_mode) +
      ("  throttling scheduler macrotask buffer size: %s\n" %
        self.throttling_scheduler_macrotask_buffer_size) +
      ("  cores per worker: %s\n" % self.num_cores) +
      ("  network bandwidth: %s B/ms\n" % self.network_bandwidth_Bpms) +
      ("  network bandwidth variance: %s\n" % self.network_bandwidth_variance) +
      ("  network latency: %s ms\n" % self.network_latency_ms) +
      ("  disks:%s\n" % (" None" if len(self.disks) == 0 else ("\n    %s" % disk_description))) +
      ("  jobs:\n%s" % SimulationConf.__format_job_info(self.jobs, indent_level="  ")))

  @staticmethod
  def format_disk_info(disks, separator):
    """
    Returns a string listing the provided disks and their write and read throughputs (in MB/s), with
    elements delimited by the given separator string.
    """
    def format_disk_info_element(disk_info):
      (disk_id, (write_throughput, read_throughput)) = disk_info
      return "%s: (write: %s MB/s, read: %s MB/s)" % (
        str(disk_id), write_throughput, read_throughput)

    return separator.join([format_disk_info_element(disk_info) for disk_info in disks.iteritems()])

  @staticmethod
  def __format_job_info(jobs, indent_level):
    """
    Returns a string listing of all of the Stages, Macrotasks, and Monotasks in the provided list of
    Jobs. Each level of the hierarchy is indented by a multiple of indent_level.
    """
    description = ""
    for job in jobs:
      stages = job.stages
      description += "%s%s: %s stages\n" % (2 * indent_level, job, len(stages))
      for stage in stages:
        macrotasks = stage.macrotasks
        description += "%s%s: %s macrotasks\n" % (3 * indent_level, stage, len(macrotasks))
        for macrotask in macrotasks:
          monotasks = macrotask.monotasks
          description += "%s%s: %s monotasks\n" % (4 * indent_level, macrotask, len(monotasks))
          for monotask in monotasks:
            description += "%s%s\n" % (5 * indent_level, monotask)
    return description

  @staticmethod
  def get_compute_time_ms(average_compute_time_ms, compute_variance):
    """
    Returns a compute time (in ms) chosen uniformly at random from within the specified variance
    bounds of the provided average compute time.
    """
    return random.uniform(
      average_compute_time_ms * (1 - compute_variance),
      average_compute_time_ms * (1 + compute_variance))

  def get_throughput_Bpms_for_disk(self, disk_id, is_write):
    """
    If is_write is True, returns the write throughput of the specified disk, otherwise returns the
    disk's read throughput. Return values are in B/ms.
    """
    write_throughput_Bpms, read_throughput_Bpms = self.disks[disk_id]
    return write_throughput_Bpms if is_write else read_throughput_Bpms


class XMLSimulationConf(SimulationConf):
  """A SimulationConf that reads configuration parameters and Job information from an XML file. """

  def __init__(self, conf_file_path):
    SimulationConf.__init__(self)
    self.load_conf_file(conf_file_path)

  def load_conf_file(self, conf_file_path):
    """Loads a configuration file into this XMLSimulationConf object.

    Initializes all instance variables based on the values in the provided configuration file.
    Creates the list of Jobs that a Simulator will execute.
    """
    dom = minidom.parse(conf_file_path)
    simulator_dom = dom.getElementsByTagName("simulator")[0]

    self.num_workers = self.__parse_int(simulator_dom, "num_workers")

    self.scheduling_mode = self.__parse_string(simulator_dom, "scheduling_mode")
    self.throttling_scheduler_macrotask_buffer_size = XMLSimulationConf.__parse_optional(
      simulator_dom, "throttling_scheduler_macrotask_buffer_size", int, 0)

    self.num_cores = self.__parse_int(simulator_dom, "num_cores_per_worker")

    network_bandwidth_Mbps = self.__parse_float(simulator_dom, "worker_network_bandwidth_Mbps")
    self.network_bandwidth_Bpms = network_bandwidth_Mbps * 1000 / 8

    self.network_bandwidth_variance = XMLSimulationConf.__parse_variance(
      simulator_dom,
      tag="worker_network_bandwidth_variance",
      default_value=0,
      error_message="packets to be transmitted at unrealistic rates")
    self.network_latency_ms = self.__parse_float(simulator_dom, "worker_network_latency_ms")

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
      disk_id = XMLSimulationConf.__parse_string(disk_dom, "id")
      write_throughput_MBps = XMLSimulationConf.__parse_float(disk_dom, "write_throughput_MBps")
      read_throughput_MBps = XMLSimulationConf.__parse_float(disk_dom, "read_throughput_MBps")
      # Multiply by 1000 to convert from MB/s to B/ms.
      disks[disk_id] = (write_throughput_MBps * 1000, read_throughput_MBps * 1000)
    return disks

  @staticmethod
  def __parse_jobs(jobs_dom, disk_ids):
    """ Returns a list of Job objects parsed from the provided DOM. """
    jobs = []
    for job_dom in jobs_dom.getElementsByTagName("job"):
      job = task_constructs.Job()
      XMLSimulationConf.__parse_stages(job_dom.getElementsByTagName("stages")[0], disk_ids, job)
      jobs.append(job)
    logging.info("Found %s Job(s)", len(jobs))
    return jobs

  @staticmethod
  def __parse_stages(stages_dom, disk_ids, job):
    """ Creates Stages for the provided Job, parsed from the provided DOM. """
    for stage_dom in stages_dom.getElementsByTagName("stage"):
      num_partitions = XMLSimulationConf.__parse_int(stage_dom, "num_partitions")
      monotasks_dom = stage_dom.getElementsByTagName("monotasks_per_partition")[0]
      stage = task_constructs.Stage(job)
      XMLSimulationConf.__parse_macrotasks(stage, num_partitions, monotasks_dom, disk_ids)

  @staticmethod
  def __parse_macrotasks(stage, num_partitions, monotasks_dom, disk_ids):
    """
    Creates num_partitions Macrotasks for the provided Stage using the template in monotasks_dom.
    """
    monotask_doms = monotasks_dom.getElementsByTagName("monotask")
    for _ in xrange(num_partitions):
      macrotask = task_constructs.Macrotask(stage)
      dag_id_to_monotask = {}
      monotask_to_dependency_dag_ids = {}
      # Reparse all of the monotasks for each Macrotask so that if the Macrotask contains disk
      # read monotasks, they are not all configured to read from the same disks.
      for monotask_dom in monotask_doms:
        monotask = XMLSimulationConf.__parse_monotask(
          monotask_dom, macrotask, num_partitions, disk_ids)
        # The string dag_id specified in the configuration file is independent of the numeric
        # monotask_id defined in the Monotask class because monotask_id must be unique for all
        # Monotasks, whereas dag_id is only unique within a Stage (for ease of configuration).
        dag_id_to_monotask[XMLSimulationConf.__parse_string(monotask_dom, "dag_id")] = monotask

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

  @staticmethod
  def __parse_monotask(monotask_dom, macrotask, num_partitions, disk_ids):
    """
    Returns a Monotask object parsed from the provided DOM that has been initialized to be part of
    the specified Macrotask. Does not set up the Monotask's dependencies.
    """
    monotask_type = XMLSimulationConf.__parse_string(monotask_dom, "type")
    if monotask_type == "compute":
      average_compute_time_ms = XMLSimulationConf.__parse_float(monotask_dom, "compute_time_ms")

      compute_variation = XMLSimulationConf.__parse_variance(
        monotask_dom,
        tag="compute_variation",
        default_value=0,
        error_message="compute monotasks to take an unrealistic amount of time to run")
      compute_time_ms = SimulationConf.get_compute_time_ms(
        average_compute_time_ms, compute_variation)

      # NetworkMonotasks are specified implicitly by defining a shuffle dependency. We cannot
      # explicitly specify NetworkMonotasks because we do not know where the shuffle data is
      # located.
      total_shuffle_bytes, is_shuffle_data_on_disk = (
        XMLSimulationConf.__parse_shuffle_dependency_info(monotask_dom, disk_ids))

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
      XMLSimulationConf.__verify_disks_exist(disk_ids)
      is_write = XMLSimulationConf.__parse_bool(monotask_dom, "is_write")
      data_size_bytes = XMLSimulationConf.__parse_int(monotask_dom, "data_size_bytes")
      disk_monotask = task_constructs.DiskMonotask(macrotask, data_size_bytes, is_write)

      if not is_write:
        # Assume the data to be read is evenly distributed across disks. Select a disk id in round
        # robin order.
        disk_monotask.disk_id = disk_ids[disk_monotask.monotask_id % len(disk_ids)]
      return disk_monotask
    else:
      raise Exception("Unknown monotask type: %s" % monotask_type)

  @staticmethod
  def __verify_disks_exist(disk_ids):
    """
    Verifies that disk accesses are possible by checking if the provided list of disks is nonempty.
    """
    if len(disk_ids) == 0:
      raise Exception("Cannot perform a disk access if no disks are specified!")

  @staticmethod
  def __parse_shuffle_dependency_info(monotask_dom, disk_ids):
    """
    Returns a tuple of (total shuffle data size bytes, is on disk?) extracted from the shuffle
    dependency info in the provided Monotask DOM, or (0, False) if the Monotask does not have a
    shuffle dependency.
    """
    shuffle_dependency_elements = monotask_dom.getElementsByTagName("shuffle_dependency")
    if len(shuffle_dependency_elements) != 1:
      return (0, False)

    shuffle_dependency_element = shuffle_dependency_elements[0]
    total_size_bytes = XMLSimulationConf.__parse_long(
      shuffle_dependency_element, "total_size_bytes")
    is_on_disk = XMLSimulationConf.__parse_bool(shuffle_dependency_element, "is_on_disk")

    if is_on_disk:
      XMLSimulationConf.__verify_disks_exist(disk_ids)
    return (total_size_bytes, is_on_disk)

  @staticmethod
  def __parse_bool(dom, tag):
    """ Extracts the boolean corresponding to the given tag in the provided DOM. """
    return XMLSimulationConf.__parse_string(dom, tag) == "True"

  @staticmethod
  def __parse_float(dom, tag):
    """ Extracts the float corresponding to the given tag in the provided DOM. """
    return float(XMLSimulationConf.__parse_string(dom, tag))

  @staticmethod
  def __parse_int(dom, tag):
    """ Extracts the int corresponding to the given tag in the provided DOM. """
    return int(XMLSimulationConf.__parse_string(dom, tag))

  @staticmethod
  def __parse_long(dom, tag):
    """ Extracts the long corresponding to the given tag in the provided DOM. """
    return long(XMLSimulationConf.__parse_string(dom, tag))

  @staticmethod
  def __parse_string(dom, tag):
    """ Extracts the string corresponding to the given tag in the provided DOM. """
    try:
      return dom.getElementsByTagName(tag)[0].firstChild.data
    except IndexError:
      raise Exception("No element with tag: %s" % tag)

  @staticmethod
  def __parse_variance(dom, tag, default_value, error_message):
    """
    Extracts the variance value corresponding to the given tag, if it exists, otherwise returns the
    provided default value. Raises an exception with the provided error message if the variance is
    not in the range [0, 1).
    """
    variance = XMLSimulationConf.__parse_optional(dom, tag, float, default_value)
    if (variance < 0) or (variance >= 1):
      raise Exception(("The %s parameter must be in the range [0, 1), otherwise it is possible " +
        "for %s.") % (tag, error_message))
    return variance

  @staticmethod
  def __parse_optional(dom, tag, type_cast, default_value):
    """
    Extracts the value corresponding to the given tag from the provided DOM and casts it to the
    specified type, or returns the given default value if the tag cannot be found.
    """
    elements = dom.getElementsByTagName(tag)
    if len(elements) > 0:
      return type_cast(elements[0].firstChild.data)
    else:
      return default_value
