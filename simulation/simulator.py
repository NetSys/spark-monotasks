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
"""Contains the core logic of the Monotasks simulator.

This file performs the joint duties of orchestrating the simulation by executing simulation Events
and mimicking the Monotasks master node. Execute the command "python simulator.py -h" from the
containing directory for instructions on running the Monotasks simulator.
"""

import argparse
import logging
from os import path
import Queue
import random

import events
import simulation_conf
import task_constructs
import worker


def main():
  args = parse_args()

  logging.basicConfig(level=args.log_level)
  logging.info("Starting Simulator using configuration file: %s", args.conf_file)
  logging.info("Using log level: %s", args.log_level)

  # Initialize the "random" module's seed value to 0 so that multiple runs of the Simulator use the
  # same pseudo-random numbers.
  random.seed(0)

  simulate(
    args.continuous_monitor_dir,
    args.continuous_monitor_interval_ms,
    simulation_conf.XMLSimulationConf(args.conf_file))


def parse_args():
  parser = argparse.ArgumentParser(description="Monotasks Simulator")
  parser.add_argument(
    "-c",
    "--conf-file",
    help="The path to a simulation configuration XML file.",
    required=True)
  parser.add_argument(
    "-o",
    "--continuous-monitor-dir",
    help="The directory that the continuous monitor logs should be written to.",
    required=True)
  parser.add_argument(
    "-l",
    "--log-level",
    choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    default="INFO",
    help="The verbosity of standard logging. See the \"logging\" package for more information.")
  parser.add_argument(
    "-i",
    "--continuous-monitor-interval-ms",
    default=50.0,
    help="The interval (in ms) between continuous monitor log entries.",
    type=float)
  return parser.parse_args()


def simulate(continuous_monitor_dir, continuous_monitor_interval_ms, conf):
  """Simulates the provided configuration.

  Records continuous monitor log entries in the provided directory at the provided frequency.

  Returns:
    The finished Simulator.
  """
  logging.debug("Simulating configuration:\n%s", conf)
  simulator = Simulator(conf, continuous_monitor_dir)
  try:
    simulator.run(continuous_monitor_interval_ms)
  finally:
    # Make sure that the continuous monitor files are always closed.
    simulator.cleanup()
  return simulator


class Simulator(object):
  """Contains the core logic of the Monotasks simulator.

  Performs the joint duties of orchestrating the Monotasks simulation and mimicking the functions
  of the Monotasks master node. Processes a queue of simulation Events and distributes Macrotasks to
  several Workers.
  """

  def __init__(self, conf, continuous_monitor_dir):
    logging.info("Saving continuous monitor logs to directory: %s", continuous_monitor_dir)

    self.conf = conf
    num_workers = self.conf.num_workers
    logging.debug("Creating Simulator with %s Worker(s)", num_workers)

    self.workers = [
      worker.Worker(self, self.conf, continuous_monitor_dir) for _ in xrange(num_workers)]
    # The Event queue contains elements of the form (Event time ms, Event object) and is serviced
    # in increasing order of Event time.
    self.event_queue = Queue.PriorityQueue()
    # A list of the Jobs that this Simulator will execute. Jobs must be executed sequentially.
    self.jobs = self.conf.jobs
    self.current_job = None
    self.current_stage = None
    # A mapping from Job to a tuple of (ideal Job completion time (in ms), actual Job completion
    # time (in ms)).
    self.job_to_jcts = {}

    # A log file in which to record simulation configuration info and results.
    self.info_file = open(path.join(continuous_monitor_dir, "simulation_info.txt"), "w")
    self.info_file.write("%s\n" % str(self.conf))

  def run(self, log_interval_ms):
    """ Continuously pops Events from the Event queue and processes them. """
    first_job = self.__get_next_job()
    if first_job is None:
      return

    current_time_ms = 0.0
    log_continuous_monitors_event = events.LogContinuousMonitors(self.workers, log_interval_ms)
    self.event_queue.put((current_time_ms, log_continuous_monitors_event))
    self.event_queue.put((current_time_ms, events.JobStart(self, first_job)))

    while not self.__is_finished():
      current_time_ms, event = self.event_queue.get(block=False)
      logging.debug("%s: Processing Event: %s", current_time_ms, event)

      new_events_and_times = event.run(current_time_ms)

      logging.debug("%s: Adding new Events: %s", current_time_ms, new_events_and_times)
      for new_event in new_events_and_times:
        self.event_queue.put(new_event)

    # Create a log entry recording the final state of the Simulator.
    log_continuous_monitors_event.run(current_time_ms)

    logging.info("Simulation complete!")
    self.__validate_bytes_sent_and_received()
    self.__record_simulation_info()


  def __validate_bytes_sent_and_received(self):
    """Verifies that the Workers sent and received the correct number of bytes over the network.

    The total number of bytes sent by all Workers should be equal to the total number of bytes
    received by all Workers.
    """
    total_bytes_sent = sum([worker_node.total_bytes_sent for worker_node in self.workers])
    total_bytes_received = sum([worker_node.total_bytes_received for worker_node in self.workers])

    logging.info(
      "Validating the total number of bytes sent (%s) and received (%s)",
      total_bytes_sent,
      total_bytes_received)
    assert abs(total_bytes_sent - total_bytes_received) < 1, \
      ("The total number of bytes sent (%s) does not equal the total number of bytes " +
        "received (%s)") % (total_bytes_sent, total_bytes_received)

  def __record_simulation_info(self):
    """ Saves the macrotask distribution and JCT info to the simulation info log file. """
    macrotask_distribution_info = self.__get_macrotask_distribution_info()
    logging.info(macrotask_distribution_info)
    self.info_file.write("%s\n\n" % macrotask_distribution_info)

    jct_info = self.__get_jct_info()
    logging.info(jct_info)
    self.info_file.write(jct_info)

  def __get_macrotask_distribution_info(self):
    """Returns a string describing the number of Macrotasks assigned to each Worker.

    For each Stage of each Job, reports how many Macrotasks from that Stage were allocated to each
    Worker.
    """
    macrotask_distribution_info = "Macrotask Distribution:"
    for job in self.jobs:
      macrotask_distribution_info += "\n  %s:" % job
      for stage in job.stages:
        macrotask_distribution_info += "\n    %s:" % stage
        for worker_node in self.workers:
          num_macrotasks = len(
            [macrotask for macrotask in stage.macrotasks if macrotask.worker is worker_node])
          macrotask_distribution_info += "\n      %s: %s" % (worker_node, num_macrotasks)
    return macrotask_distribution_info

  def __get_jct_info(self):
    """
    Returns a string describing the ideal and actual Job completion time (in ms), as well as their
    percent difference, for each Job that this Simulator has finished executing so far.
    """
    job_descriptions = [
      ("  %s:\n    Ideal JCT: %.2f ms\n    Actual JCT: %.2f ms\n    Difference: %.2f%%" % (
        job,
        ideal_jct_ms,
        actual_jct_ms,
        float(actual_jct_ms - ideal_jct_ms) / ideal_jct_ms * 100))
      for job, (ideal_jct_ms, actual_jct_ms) in self.job_to_jcts.iteritems()]
    return "JCT Info:\n%s" % ("\n".join(job_descriptions))

  def __is_finished(self):
    """Determines whether the simulation is complete.

    The simulation is complete when self.event_queue contains only one item, which is guaranteed to
    be a LogContinuousMonitors Event.

    Returns:
      True if the simulation is complete, or False otherwise.
    """
    return self.event_queue.qsize() == 1

  def __get_next_job(self):
    """Retrieves the next Job to execute.

    Returns:
      The first unfinished Job, or None if all Jobs have finished. """
    return next((job for job in self.jobs if not job.is_finished()), None)

  def start_job(self, current_time_ms, job):
    """Starts the first Stage in the provided Job.

    Returns:
      MacrotaskStart Events for any Macrotasks in the provided Job's first Stage that were assigned
      to Workers.
    """
    self.current_job = job
    self.current_job.start_time_ms = current_time_ms
    logging.info("%s: Starting %s", current_time_ms, self.current_job)
    return self.__start_next_stage(current_time_ms)

  def __start_next_stage(self, current_time_ms):
    """Attempts to start the next Stage of the current Job.

    Checks if there are more Stages in the current Job. If yes, starts the next Stage. If no, starts
    the next Job.

    Returns:
      MacrotaskStart Events for any Macrotasks that were assigned to Workers. If there are no more
      Stages in the current Job and there are more Jobs remaining, returns a JobStart Event for the
      next Job.
    """
    next_stage = self.current_job.get_next_stage()
    if next_stage is None:
      # There are no more Stages in the current Job, so it has finished. Try to start the next Job.
      logging.info("%s: No more Stages in %s", current_time_ms, self.current_job)

      self.job_to_jcts[self.current_job] = (
        self.current_job.calculate_ideal_completion_time_ms(self.conf, self.info_file),
        current_time_ms - self.current_job.start_time_ms)
      self.current_job = None

      next_job = self.__get_next_job()
      if next_job is None:
        logging.info("%s: No more Jobs", current_time_ms)
        return []
      else:
        # Start the next Job.
        return [(current_time_ms, events.JobStart(self, next_job))]
    else:
      # Start the next Stage.
      self.current_stage = next_stage
      logging.info("%s: Starting %s", current_time_ms, self.current_stage)
      return self.__schedule_initial_macrotasks(current_time_ms)

  def __schedule_initial_macrotasks(self, current_time_ms):
    """Sends each Worker its desired initial number of Macrotasks.

    Called when a Stage starts. Load balances the Macrotasks in the current Stage across the Workers
    without sending any Worker more than its desired initial number of Macrotasks.

    Returns:
      MacrotaskStart Events for any Macrotasks that were accepted by Workers.
    """
    macrotask_scheduled_in_last_iteration = True
    new_events = []
    # Cycle through the Workers in round robin order, attempting to assign one Macrotask to each
    # Worker until either all Macrotasks have been assigned or all Workers have accepted as many
    # Macrotasks as they can. This strategy load balances the Macrotasks across the Workers.
    while macrotask_scheduled_in_last_iteration:
      macrotask_scheduled_in_last_iteration = False
      for worker_node in self.workers:
        if (worker_node.num_assigned_macrotasks <
            worker_node.scheduler.get_num_initial_macrotasks(self.current_stage)):
          new_event = self.send_macrotask_to_worker(current_time_ms, worker_node)
          if len(new_event) == 1:
            # Since a Worker accepted a Macrotask, we signal that we should try another iteration in
            # case this Worker can accept another Macrotask. We do not immediately try to assign
            # another Macrotask to this Worker because we want to load balance the Macrotasks across
            # the Workers.
            macrotask_scheduled_in_last_iteration = True
            new_events.extend(new_event)

    return new_events

  def send_macrotask_to_worker(self, current_time_ms, worker_node):
    """
    If there are unassigned Macrotasks in the current Stage, then this method sends one to the given
    Worker.

    Returns:
      A list containing a MacrotaskStart Event for when a Macrotask will arrive at the specified
      Worker, or an empty list if there are no more Macrotasks in the current Stage.
    """
    macrotask_to_assign = self.current_stage.get_next_macrotask()
    if macrotask_to_assign is None:
      # All Macrotasks for this Stage have been scheduled, so we do nothing.
      logging.info(
        "%s: All Macrotasks in %s have been assigned to Workers.",
        current_time_ms,
        self.current_stage)
      return []
    else:
      macrotask_to_assign.worker = worker_node
      worker_node.num_assigned_macrotasks += 1
      logging.info("%s: %s accepted by %s", current_time_ms, macrotask_to_assign, worker_node)

      # Create a MacrotaskStart Event to signal that the Macrotask has arrived at the Worker.
      arrival_time_ms = current_time_ms + worker_node.conf.network_latency_ms
      return [(arrival_time_ms, events.MacrotaskStart(macrotask_to_assign))]

  def finish_macrotask(self, current_time_ms, macrotask):
    """Registers that the provided Macrotask completed.

    Only schedules a new Macrotask if the current Stage has finished and a new Stage should start.
    This may require starting a new Job, if there are more Jobs remaining. Scheduling new Macrotasks
    while a Stage is running is done via MacrotaskRequest Events.

    Returns:
      A list containing MacrotaskStart Events if a new Stage starts or a JobStart Event if a new Job
      starts, or an empty list if there are no more Stages or Jobs remaining.
    """
    macrotask.worker.num_assigned_macrotasks -= 1
    macrotask.master_knows_is_finished = True
    if self.current_stage.is_finished():
      # There are no more Macrotasks in the current Stage, so it has finished. Try to start the next
      # Stage.
      logging.info("%s: No more Macrotasks in %s", current_time_ms, self.current_stage)
      self.current_stage = None
      return self.__start_next_stage(current_time_ms)
    else:
      # We do not start a new Macrotask because once a Stage has started, Macrotasks are pulled by
      # the Workers.
      return []

  def cleanup(self):
    """ Closes the simulation info log file and all of the Workers' ContinuousMonitors. """
    self.info_file.close()
    for worker_node in self.workers:
      worker_node.continuous_monitor.close()


if __name__ == "__main__":
  main()
