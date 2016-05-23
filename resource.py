#!/usr/bin/env python
#
# Copyright 2016 Rafael Ferreira da Silva
# http://www.rafaelsilva.com/tools
#
# Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
__author__ = "Rafael Ferreira da Silva"

from controller import *
from task import *

log = logging.getLogger(__name__)


class ResourceStatus:
    IDLE = "idle"
    BUSY = "busy"


class InsufficientSpace(Exception):
    pass


class InsufficientMemory(Exception):
    pass


class ComputeResource:
    def __init__(self, id, accepted_tasks=[], shared_storage=None, local_storage_capacity=0, memory_capacity=1000):
        """

        :param id:
        :param accepted_tasks:
        :param shared_storage:
        :param local_storage_capacity:
        :param memory_capacity:
        """
        self.id = id
        self.accepted_tasks = accepted_tasks
        self.shared_storage = shared_storage
        if local_storage_capacity > 0:
            self.local_storage = Storage(local_storage_capacity)
        else:
            self.local_storage = None
        self.memory = {
            'capacity': int(memory_capacity),
            'available': int(memory_capacity)
        }
        self.compute_units = {}
        self.mem_controller = None

    def generate_compute_units(self, compute_units=20):
        """

        :param compute_units:
        :return:
        """
        for i in range(0, compute_units):
            self.compute_units[i] = ComputeUnit(i)

    def run_task(self, task):
        """
        Try to schedule a task to a computing node. This method verifies if the task is allowed to run in the
        current compute resource, and if there is enough storage and memory available.
        :param task: task object
        :return: compute resource where the task has been scheduled
        """
        if task.transformation not in self.accepted_tasks and task.type != TaskType.CLEANUP:
            return None

        for compute_unit in self.compute_units.values():
            if compute_unit.status == ResourceStatus.IDLE:

                if task.type != TaskType.CLEANUP:
                    # evaluate disk and memory requirements
                    required_storage = self._get_required_storage(task)

                    if (self.local_storage and self.local_storage.available < required_storage) \
                            or self.shared_storage.available < required_storage:
                        # insufficient disk space in local and shared storage
                        raise InsufficientSpace("Required storage (%s) is more than available space (%s)."
                                                % (required_storage, self.shared_storage.available))

                    if self.memory['available'] < task.peak_memory:
                        raise InsufficientMemory("[%s] Required memory (%s) is more than available memory (%s)."
                                                 % (self.id, task.peak_memory, self.memory['available']))

                    # add task files to storage
                    self._add_to_storage(task.input_data)
                    self._add_to_storage(task.intermediate_data)
                    self._add_to_storage(task.output_data)

                    # memory usage
                    self.memory['available'] -= task.peak_memory

                # run the task
                compute_unit.run_task(task)

                return compute_unit
        return None

    def process_finished_task(self, compute_unit):
        """

        :param compute_unit:
        :return:
        """
        self.memory['available'] += compute_unit.current_task.peak_memory
        self._clean_files(compute_unit.current_task)
        compute_unit.process_finished_task()

    def preempt_task(self, task, required_files):
        """
        Preempt a task and remove its files.
        :param task: task to be preempted
        :param required_files: list of files that should not be removed during preemption
        :return: preempted task
        """
        # find the compute node where the task is running
        cu_to_preempt = None
        for compute_unit in self.compute_units.values():
            if compute_unit.current_task == task:
                compute_unit.preempt_task()
                self._clean_files(task, required_files)
                self.memory['available'] += task.peak_memory
                return task

        return None

    def get_list_of_current_used_files(self):
        """
        Get list of used files by current running tasks.
        :return: list of current used files
        """
        required_files = []
        for cu in self.compute_units.values():
            if cu.status == ResourceStatus.BUSY:
                if cu.current_task.type == TaskType.CLEANUP:
                    continue
                for l in [cu.current_task.input_data, cu.current_task.intermediate_data, cu.current_task.output_data]:
                    for f in l.values():
                        if f not in required_files:
                            required_files.append(f)

        return required_files

    def get_running_tasks(self):
        running_tasks = []
        for cu in self.compute_units.values():
            if cu.status == ResourceStatus.BUSY:
                if cu.current_task.type == TaskType.CLEANUP:
                    continue
                running_tasks.append(cu.current_task)

        return running_tasks

    def set_mem_controller(self, memory_threshold=0.8, kp=1, ki=1, kd=1):
        """
        Set a memory controller.
        :param memory_threshold: memory settling point
        :param kp: proportional constant
        :param ki: integral constant
        :param kd: derivative constant
        """
        self.mem_controller = Controller(memory_threshold * self.memory['capacity'], kp=kp, ki=ki, kd=kd)

    def get_current_used_memory(self):
        return self.memory['capacity'] - self.memory['available']

    def get_mem_controller_input(self):
        """
        Compute memory controller input variable from current memory usage.
        :return: memory input value from controller
        """
        controller_input = self.mem_controller.process(self.get_current_used_memory())
        if controller_input > self.memory['capacity']:
            return self.memory['capacity']

        return controller_input

    def _get_required_storage(self, task):
        """
        Get required amount of disk space to run the task. This method checks whether the data (or part of it) is
        already available in the local or shared storage.
        :param task: task object
        :return: required amount of disk space
        """
        required_storage = 0
        for l in [task.input_data, task.intermediate_data, task.output_data]:
            for f in l.values():
                if (self.local_storage and f.name not in self.local_storage.files) \
                        or f.name not in self.shared_storage.files:
                    required_storage += f.size

        return required_storage

    def _add_to_storage(self, files_list):
        """
        Add files to local (priority) or shared storage.
        :param files_list: list of files
        """
        for f in files_list.values():
            storage = self.shared_storage
            if self.local_storage:
                storage = self.local_storage
            if f.name not in storage.files:
                storage.files[f.name] = f
                storage.available -= f.size

    def _clean_files(self, task, required_files=None):
        """
        Only remove files that are not used by current tasks.
        :param task: task object
        :param required_files: list of required files that should not be removed
        """
        tasks_to_be_removed = []
        tasks_to_be_removed.extend(task.input_data.values())

        if task.type != TaskType.CLEANUP:
            tasks_to_be_removed.extend(task.intermediate_data.values())

        # do not remove files required by pending tasks
        if required_files:
            for f in required_files:
                if f in tasks_to_be_removed:
                    tasks_to_be_removed.remove(f)

        # do not remove files required by current running tasks
        for cu in self.compute_units.values():
            if cu.status == ResourceStatus.BUSY:
                if cu.current_task.type == TaskType.CLEANUP or cu.current_task.id == task.id:
                    continue
                for l in [cu.current_task.input_data, cu.current_task.intermediate_data, cu.current_task.output_data]:
                    for f in l.values():
                        if f in tasks_to_be_removed:
                            tasks_to_be_removed.remove(f)

        # remove files
        for f in tasks_to_be_removed:
            if self.local_storage and f in self.local_storage.files.values():
                self.local_storage.available += f.size
                del self.local_storage.files[f.name]

            elif f in self.shared_storage.files.values():
                self.shared_storage.available += f.size
                del self.shared_storage.files[f.name]
                # print "REMOVED FROM SHARED: %s" % f

                # if required_files:
                #     print "[Required files] %s" % print_dictionary_ids(required_files)
                # print "[Files to be removed] %s" % print_dictionary_ids(tasks_to_be_removed)

    def __str__(self):
        """
        Print the compute resource properties.
        :return: compute resource properties in string format
        """
        str = "Resource {\n"
        str += "  id: %s\n" % self.id
        # str += "  local storage:\n"
        # str += "    capacity: %s\n" % self.local_storage['capacity']
        # str += "    available: %s\n" % self.local_storage['available']
        # str += "    files: (%s)\n" % print_dictionary_ids(self.local_storage['files'])
        if self.shared_storage:
            str += "  shared storage:\n"
            str += "    capacity: %s\n" % self.shared_storage.capacity
            str += "    available: %s\n" % self.shared_storage.available
            str += "    files: (%s)\n" % print_dictionary_ids(self.shared_storage.files)
        str += "  memory:\n"
        str += "    capacity: %s\n" % self.memory['capacity']
        str += "    available: %s\n" % self.memory['available']
        str += "  compute_units:\n"
        for cu in self.compute_units.values():
            if cu.status == ResourceStatus.BUSY:
                str += "    %s\n" % cu
        str += "}"
        return str


class ComputeUnit:
    def __init__(self, id):
        self.id = id
        self.status = ResourceStatus.IDLE
        self.current_task = None

    def run_task(self, task):
        self.status = ResourceStatus.BUSY
        self.current_task = task

    def process_finished_task(self):
        self.status = ResourceStatus.IDLE
        self.current_task.status = TaskStatus.COMPLETED
        self.current_task = None

    def preempt_task(self):
        self.status = ResourceStatus.IDLE
        self.current_task.preempt()
        self.current_task = None

    def __str__(self):
        current_task_id = None
        if self.current_task:
            current_task_id = self.current_task.id
        return "CU: {id: %s, status: %s, current_task_id: %s}" % (self.id, self.status, current_task_id)


class Storage:
    def __init__(self, capacity):
        self.capacity = capacity
        self.available = capacity
        self.files = {}

    def current_used_storage(self):
        """
        Get current storage usage.
        :return:
        """
        return self.capacity - self.available
