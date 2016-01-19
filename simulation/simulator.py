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
import Queue
import random

import events
import simulation_conf
import worker


def main():
  args = parse_args()

  logging.basicConfig(level=args.log_level)
  logging.info("Starting Simulator using configuration file: %s", args.conf_file)
  logging.info("Using log level: %s", args.log_level)
  logging.info("Saving continuous monitor logs to directory: %s", args.continuous_monitor_dir)

  # Initialize the "random" module's seed value to 0 so that multiple runs of the Simulator use the
  # same pseudo-random numbers.
  random.seed(0)

  simulator = Simulator(simulation_conf.SimulationConf(args.conf_file), args.continuous_monitor_dir)
  try:
    simulator.run(args.log_interval_ms)
  finally:
    simulator.cleanup()


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
    "--log-interval-ms",
    default=50.0,
    help="The interval (in ms) between continuous monitor log entries.",
    type=float)
  return parser.parse_args()


class Simulator(object):
  """Contains the core logic of the Monotasks simulator.

  Performs the joint duties of orchestrating the Monotasks simulation and mimicking the functions
  of the Monotasks master node. Processes a queue of simulation Events and distributes Macrotasks to
  several Workers.
  """

  def __init__(self, conf, continuous_monitor_dir):
    num_workers = conf.num_workers
    logging.debug("Creating Simulator with %s Worker(s)", num_workers)

    self.workers = [worker.Worker(self, conf, continuous_monitor_dir) for _ in xrange(num_workers)]
    # The Event queue contains elements of the form (Event time ms, Event object) and is serviced
    # in increasing order of Event time.
    self.event_queue = Queue.PriorityQueue()
    # A deque of Jobs waiting to be executed. Uses a deque instead of a list because Jobs are
    # executed sequentially and once a Job is finished we do not need to keep its data structures
    # around.
    self.waiting_jobs = conf.jobs
    self.current_job = None
    self.current_stage = None
    # A mapping from Job ID to Job completion time. For testing purposes.
    self.jcts = {}

  def run(self, log_interval_ms):
    """ Continuously pops Events from the Event queue and processes them. """
    if len(self.waiting_jobs) == 0:
      return

    current_time_ms = 0.0
    log_continuous_monitors_event = events.LogContinuousMonitors(self.workers, log_interval_ms)
    self.event_queue.put((current_time_ms, log_continuous_monitors_event))
    self.event_queue.put((current_time_ms, events.JobStart(self, self.waiting_jobs.popleft())))

    while not self.__is_finished():
      current_time_ms, event = self.event_queue.get(block=False)
      logging.debug("%s: Processing Event: %s", current_time_ms, event)

      new_events_and_times = event.run(current_time_ms)

      logging.debug("%s: Adding new Events: %s", current_time_ms, new_events_and_times)
      for new_event in new_events_and_times:
        self.event_queue.put(new_event)

    # Create a log entry recording the final state of the Simulator.
    log_continuous_monitors_event.run(current_time_ms)

    for worker_node in self.workers:
      worker_node.validate_bytes_sent_and_received()
    logging.info("Simulation complete!")

  def __is_finished(self):
    """Determines whether the simulation is complete.

    The simulation is complete when self.event_queue contains only one item, which is guaranteed to
    be a LogContinuousMonitors Event.

    Returns:
      True if the simulation is complete, or False otherwise.
    """
    return self.event_queue.qsize() == 1

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
    if len(self.current_job.waiting_stages) == 0:
      # There are no more Stages in the current Job, so it is finished.
      logging.info("%s: No more Stages in %s", current_time_ms, self.current_job)
      self.jcts[self.current_job.job_id] = current_time_ms - self.current_job.start_time_ms
      self.current_job = None

      if len(self.waiting_jobs) == 0:
        logging.info("%s: No more Jobs", current_time_ms)
        return []
      else:
        # Start the next Job.
        return [(current_time_ms, events.JobStart(self, self.waiting_jobs.popleft()))]
    else:
      # Start the next Stage.
      self.current_stage = self.current_job.waiting_stages.popleft()
      logging.info("%s: Starting %s", current_time_ms, self.current_stage)
      return self.__schedule_macrotasks(current_time_ms)

  def finish_macrotask(self, current_time_ms, macrotask):
    """Registers that the provided Macrotask completed.

    Potentially starts one or more new Macrotasks. This may involve starting a new Stage or Job.

    Returns:
      MacrotaskStart Events for any Macrotasks that were accepted by Workers, or a JobStart Event if
      the provided Macrotask is the last in its Job and there are more Jobs.
    """
    if macrotask in self.current_stage.waiting_macrotasks:
      self.current_stage.waiting_macrotasks.remove(macrotask)
    else:
      raise Exception("Macrotask %s was removed from %s's list of Macrotasks before it completed." %
        (macrotask, self.current_stage))

    if len(self.current_stage.waiting_macrotasks) == 0:
      # There are no more Macrotasks in the current Stage, so it is finished. Start the next Stage.
      logging.info("%s: No more Macrotasks in %s", current_time_ms, self.current_stage)
      self.current_stage = None
      return self.__start_next_stage(current_time_ms)
    else:
      return self.__schedule_macrotasks(current_time_ms)

  def __schedule_macrotasks(self, current_time_ms):
    """
    Attempts to distribute the remaining Macrotasks for the current Stage amongst the Workers.
    Returns MacrotaskStart Events for any Macrotasks that were accepted by Workers.
    """
    unscheduled_macrotasks = [macrotask for macrotask in self.current_stage.waiting_macrotasks
      if not macrotask.assigned_to_worker]

    macrotask_scheduled_in_last_iteration = True
    new_events = []
    # Cycle through the Workers in round robin order, attempting to assign one Macrotask to each
    # Worker until either all Macrotasks have been assigned or all Workers have accepted as many
    # Macrotasks as they can. This strategy load balances the Macrotasks across the Workers.
    while macrotask_scheduled_in_last_iteration:
      macrotask_scheduled_in_last_iteration = False
      for worker_node in self.workers:
        if len(unscheduled_macrotasks) == 0:
          return new_events

        macrotask_to_submit = unscheduled_macrotasks[0]
        if worker_node.num_running_macrotasks < worker_node.max_macrotasks:
          logging.info("%s: %s accepted by %s", current_time_ms, macrotask_to_submit, worker_node)
          worker_node.num_running_macrotasks += 1
          macrotask_to_submit.assigned_to_worker = True
          unscheduled_macrotasks.remove(macrotask_to_submit)

          # Since a Worker accepted a Macrotask, we signal that we should try another iteration in
          # case this Worker can accept another Macrotask. We do not immediately try to assign
          # another Macrotask to this Worker because we want to load balance the Macrotasks across
          # the Workers.
          macrotask_scheduled_in_last_iteration = True

          # Create a MacrotaskStart Event to signal that the Macrotask itself has arrived at the
          # Worker.
          arrival_time_ms = current_time_ms + worker_node.conf.network_latency_ms
          new_events.append(
            (arrival_time_ms, events.MacrotaskStart(worker_node, macrotask_to_submit)))

    return new_events

  def cleanup(self):
    """ Closes all of the Workers' ContinuousMonitors. """
    for worker_node in self.workers:
      worker_node.continuous_monitor.close()


if __name__ == "__main__":
  main()
