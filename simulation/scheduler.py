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

import abc
import math

import events
import task_constructs


class Scheduler(object):
  """A module that controls when Macrotasks are assigned to Workers.

  Each Worker is paired with a Scheduler, which requests Macrotasks from the master when Monotasks
  complete. Subclasses implement various scheduling policies.

  This class only implements get_scheduler_for_mode() and get_macrotask_request().
  """

  def __init__(self, worker):
    # The Worker that this Scheduler manages.
    self.worker = worker

  @staticmethod
  def get_scheduler_for_mode(mode, worker):
    """
    Returns a new Scheduler object the implements the provided mode, configured to manage the
    provided Worker.
    """
    if mode == "even-distribution":
      return EvenDistributionScheduler(worker)
    elif mode == "fixed-slots":
      return FixedSlotsScheduler(worker)
    elif mode == "throttling":
      return ThrottlingScheduler(worker)
    else:
      raise Exception("Unknown scheduling mode: %s" % mode)

  @abc.abstractmethod
  def handle_macrotask_start(self, first_macrotask):
    """
    Updates internal state to reflect that a Macrotask has arrived at this Scheduler's Worker.
    """

  @abc.abstractmethod
  def handle_monotask_end(self, current_time_ms, completed_monotask):
    """
    Handles completion of a Monotask and returns a potentially empty list of MacrotaskRequest
    Events.
    """

  @abc.abstractmethod
  def get_num_initial_macrotasks(self, stage):
    """ Returns the number of Macrotasks to send this Scheduler's Worker when a Stage starts. """

  def get_macrotask_request(self, current_time_ms):
    """
    Returns a new MacrotaskRequest Event for this Scheduler's Worker, scheduled for the current time
    plus the network latency between the Worker and the master.
    """
    return (
      current_time_ms + self.worker.conf.network_latency_ms,
      events.MacrotaskRequest(self.worker))


class EvenDistributionScheduler(Scheduler):
  """A naive Scheduler that assigns the same number of Macrotasks to each Worker.

  All of the Macrotasks in a Stage are divided evenly among the Workers and assigned as soon as the
  Stage starts.
  """

  def __repr__(self):
    return "EvenDistributionScheduler for %s" % self.worker

  def handle_macrotask_start(self, first_macrotask):
    # The EvenDistributionScheduler does not need to update any internal state when a Macrotask
    # starts.
    pass

  def handle_monotask_end(self, current_time_ms, completed_monotask):
    # All Macrotasks are distributed when a Stage starts, so there are no more Macrotasks that can
    # be requested.
    return []

  def get_num_initial_macrotasks(self, stage):
    return math.ceil(float(len(stage.macrotasks)) / self.worker.conf.num_workers)


class FixedSlotsScheduler(Scheduler):
  """A naive Scheduler that has a fixed number of Macrotask slots.

  A Worker's resource profile determines how many Macrotasks it will be sent when a Stage begins.
  Then, a new Macrotask will be sent whenever one completes.
  """

  def __repr__(self):
    return "FixedSlotsScheduler for %s" % self.worker

  def handle_macrotask_start(self, first_macrotask):
    # The FixedSlotsScheduler does not need to update any internal state when a Macrotask starts.
    pass

  def handle_monotask_end(self, current_time_ms, completed_monotask):
    if completed_monotask.macrotask.all_monotasks_finished():
      # Always request a new Macrotask when one completes.
      return [self.get_macrotask_request(current_time_ms)]
    else:
      return []

  def get_num_initial_macrotasks(self, stage):
    # The maximum number of Macrotasks that this Scheduler's Worker should be executing at a time is
    # equal to the maximum number of concurrent ComputeMonotasks plus the maximum number of
    # concurrent DiskMonotasks plus 1, to allow for a NetworkMonotask.
    return self.worker.conf.num_cores + len(self.worker.disks) + 1


class ThrottlingScheduler(Scheduler):
  """A Scheduler that dynamically throttles the rate at which Macrotasks are requested.

  A Stage's resource profile is used to build a resource pipeline through which Macrotasks can be
  thought to move. The bottleneck resource applies backpressure through the pipeline to throttle
  when Macrotasks are requested from the master node.
  """

  def __init__(self, worker):
    Scheduler.__init__(self, worker)
    self.macrotask_buffer_size = self.worker.conf.throttling_scheduler_macrotask_buffer_size
    self.current_stage = None
    self.first_phase = None
    # A mapping from Monotask to its corresponding Phase. The mapping for the roots of a DAG of
    # Monotasks are set when a Macrotask starts. The mapping for non-root Monotasks will be set when
    # one of their dependencies finishes.
    self.monotask_to_phase = {}

  def __repr__(self):
    return "ThrottlingScheduler for %s" % self.worker

  def handle_macrotask_start(self, macrotask):
    macrotask_stage = macrotask.stage
    if macrotask_stage is not self.current_stage:
      # This is the first Macrotask in a new Stage.
      self.current_stage = macrotask_stage
      self.first_phase = self.__build_phase_chain(macrotask)

    # Find the roots of this Macrotask's DAG of Monotasks and set their Phases to be the second
    # Phase.
    second_phase = self.first_phase.next_phase
    for monotask in macrotask.monotasks:
      if monotask.dependencies_have_finished():
        self.monotask_to_phase[monotask] = second_phase

    # As soon as a Macrotask arrives at a Worker, it instantly moves through the first Phase.
    self.first_phase.handle_macrotask_phase_change()

  def __build_phase_chain(self, macrotask):
    """Constructs a linked list of Phases based on the provided Macrotask's DAG of Monotasks.

    The first Phase is always a FirstPhase, which handles deciding when to request Macrotasks from
    the master node. The rest of the Phases correspond to the resources that the Macrotask (and all
    other Macrotasks from the same Stage) will use during its execution.

    Returns:
      A linked list of Phases.
    """
    # The first Phase is always a placeholder to ensure that a single Macrotask is always waiting to
    # be submitted.
    first_phase = last_phase = ThrottlingScheduler.FirstPhase(
      self.macrotask_buffer_size, self.get_num_initial_macrotasks(macrotask.stage))

    roots = [monotask for monotask in macrotask.monotasks if monotask.dependencies_have_finished()]

    # The DAG of Monotasks must, by definition, have at least one root. Multiple roots are allowed,
    # but we assume that the DAG is symmetric, meaning that all possible paths from sources to sinks
    # use the same resources in the same order.
    if len(roots) == 0:
      raise Exception("All Monotask DAGs should contain at least one Monotask that does not have " +
        "any dependencies.")

    current_monotask = roots[0]
    phase_index = 1
    while current_monotask is not None:
      if isinstance(current_monotask, task_constructs.ComputeMonotask):
        concurrency = self.worker.conf.num_cores
      elif isinstance(current_monotask, task_constructs.NetworkRequestMonotask):
        # If a Worker receives requests from one remote Worker for two different Macrotasks, it will
        # service those requests sequentially, instead of in parallel.
        concurrency = 1
      elif isinstance(current_monotask, task_constructs.NetworkResponseMonotask):
        # NetworkResponseMonotasks are not considered part of the Phase pipeline. The presence of a
        # network Phase is determined by NetworkRequestMonotasks.
        phase_index += 1
        continue
      elif isinstance(current_monotask, task_constructs.DiskMonotask):
        # TODO: This assumes that any Monotask can use any disk, which is not accurate. In order to
        #       resolve this, we need to modify the throttling algorithm to support branches in the
        #       resource phase pipeline.
        concurrency = len(self.worker.disks)
      else:
        raise Exception("Unsupported Monotask: %s" % current_monotask)

      new_phase = ThrottlingScheduler.Phase(
        phase_index, self.macrotask_buffer_size, last_phase, concurrency)
      last_phase.next_phase = new_phase
      last_phase = new_phase
      phase_index += 1

      dependents = current_monotask.dependents
      num_dependents = len(dependents)
      if num_dependents == 0:
        # This is the last Phase, so we stop iterating.
        current_monotask = None
      elif num_dependents == 1:
        current_monotask = dependents[0]
      else:
        raise Exception("Currently, only DAGs with no branches are supported.")

    return first_phase

  def handle_monotask_end(self, current_time_ms, completed_monotask):
    # NetworkResponseMonotasks are not tracked by the ThrottlingScheduler. A Macrotask moves out of
    # the network phase when its NetworkRequestMonotask finishes.
    if isinstance(completed_monotask, task_constructs.NetworkResponseMonotask):
      return []

    # Determine which Phase the completed Monotask's dependents belong to.
    phase = self.monotask_to_phase[completed_monotask]
    del self.monotask_to_phase[completed_monotask]
    next_phase = phase.next_phase
    for dependent in completed_monotask.dependents:
      if dependent in self.monotask_to_phase:
        existing_phase = self.monotask_to_phase[dependent]
        if existing_phase is not next_phase:
          raise Exception(
            "%s exists in multiple phases: %s and %s" % (dependent, existing_phase, next_phase))
      else:
        self.monotask_to_phase[dependent] = next_phase

    if phase.handle_macrotask_phase_change():
      return [self.get_macrotask_request(current_time_ms)]
    else:
      return []

  def get_num_initial_macrotasks(self, stage):
    return self.worker.conf.num_cores + 1


  class Phase(object):
    """
    A phase in the resource pipeline through which Macrotasks can be thought to travel while being
    executed. Used to keep track of when to throttle particular resources and when to request more
    Macrotasks from the master node.
    """

    def __init__(self, index, macrotask_buffer_size, previous_phase, concurrency):
      self.index = index
      # The number of extra Macrotasks that are allowed to be requested from the master node.
      self.macrotask_buffer_size = macrotask_buffer_size
      self.previous_phase = previous_phase
      self.next_phase = None
      self.concurrency = concurrency
      self.num_approved_to_start = 0
      self.num_finished = 0
      self.is_throttled = False

    def __repr__(self):
      return ("(Phase | index: %s | concurrency: %s | approved to start: %s | finished: %s | " +
        "throttled: %s)") % (
          self.index,
          self.concurrency,
          self.num_approved_to_start,
          self.num_finished,
          self.is_throttled)

    def handle_macrotask_phase_change(self):
      """Registers that a Macrotask has finished this Phase.

      Returns:
        True if another Macrotask should be requested from the master node, or False otherwise.
      """
      self.num_finished += 1

      if self.next_phase is not None:
        if ((not self.next_phase.is_throttled) and
            ((self.num_finished - self.next_phase.num_finished) <= self.next_phase.concurrency)):
          self.next_phase.num_approved_to_start += 1
        else:
          self.update_throttling()

      if not self.is_throttled:
        return self.approve_task_to_start()
      else:
        return False

    def update_throttling(self):
      """Determines whether this Phase should be throttled.

      The current phase is throttled if the queue size of the next phase grows beyond a certain
      threshold.
      """
      next_queue_allowed_len = self.concurrency + self.macrotask_buffer_size
      # The last Phase is never throttled, so only update self.is_throttled if this is not the last
      # Phase.
      if self.next_phase is not None:
        next_queue_current_len = self.num_finished - self.next_phase.num_approved_to_start
        self.is_throttled = self.is_throttled or (next_queue_current_len >= next_queue_allowed_len)

      if self.is_throttled:
        self.num_approved_to_start = self.next_phase.num_approved_to_start + next_queue_allowed_len

        if self.previous_phase is not None:
          self.previous_phase.update_throttling()

    def approve_task_to_start(self):
      """
      Updates this Phase's metadata to reflect that it is allowed to start executing another
      Macrotask.

      Returns:
        Recurses until reaching a FirstPhase object, at which point the decision on whether to
        request a Macrotask from the master node is propogated back. Returns True if a Macrotask
        should be requested from the master node, or False otherwise.
      """
      task_available_to_start = self.previous_phase.num_finished > self.num_approved_to_start
      resources_available = (self.num_approved_to_start - self.num_finished) < self.concurrency
      if task_available_to_start and resources_available:
        self.num_approved_to_start += 1
      else:
        self.is_throttled = False

      if resources_available and self.previous_phase.is_throttled:
         return self.previous_phase.approve_task_to_start()
      else:
        return False


  class FirstPhase(Phase):
    """
    The first Phase in the Macrotask resource pipeline, which handles pulling Macrotasks from the
    master node. This class contains additional metadata compared to the standard Phase class, as
    well as special handling for deciding when to request Macrotasks.
    """

    def __init__(self, macrotask_buffer_size, num_initial_macrotasks):
      ThrottlingScheduler.Phase.__init__(
        self,
        index=0,
        macrotask_buffer_size=macrotask_buffer_size,
        previous_phase=None, concurrency=1)
      self.is_throttled = True
      self.num_approved_to_start = num_initial_macrotasks
      self.num_requested_from_master = num_initial_macrotasks

    def __repr__(self):
      return (
        ("(FirstPhase | concurrency: %s | requested from master: %s | approved to start: %s " +
          "| finished: %s | throttled: %s)") % (
            self.concurrency,
            self.num_requested_from_master,
            self.num_approved_to_start,
            self.num_finished,
            self.is_throttled))

    def approve_task_to_start(self):
      self.num_approved_to_start += 1
      if self.num_approved_to_start > self.num_requested_from_master:
        self.num_requested_from_master += 1
        return True
      else:
        return False
