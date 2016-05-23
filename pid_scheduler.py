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

import random

from resource import *
from task import *

log = logging.getLogger(__name__)

# constants
STORAGE_CAPACITY = 500000
STORAGE_LIMIT = 450000
MEMORY_THRESHOLD = 0.8

STO_KP = 1.0
STO_KI = 1.0
STO_KD = 1.0
MEM_KP = 1.0
MEM_KI = 1.0
MEM_KD = 1.0

# optimization
# STO_KP = 0.35
# STO_KI = 0.22
# STO_KD = 0.14
# MEM_KP = 0.32
# MEM_KI = 0.05
# MEM_KD = 0.51

# storage estimation based on the mean
STORAGE_ESTIMATION = {
    TaskTransformation.INDIVIDUALS: 173795.35,
    TaskTransformation.SIFTING: 948.51,
    TaskTransformation.POPULATION: 0.14,
    TaskTransformation.PAIR: 1837.15,
    TaskTransformation.FREQUENCY: 1837.15
}

# memory estimation based on the mean
MEMORY_ESTIMATION = {
    # mean
    TaskTransformation.INDIVIDUALS: 411080.18,
    TaskTransformation.SIFTING: 7956.18,
    TaskTransformation.POPULATION: 1.00,
    TaskTransformation.PAIR: 18237.66,
    TaskTransformation.FREQUENCY: 8372.45
}


class PIDScheduler:
    def __init__(self, workflow, compute_resources, shared_storage):
        """

        :param workflow:
        :param compute_resources:
        :param shared_storage:
        """
        self.workflow = workflow
        self.compute_resources = compute_resources
        self.shared_storage = shared_storage
        self.disk_controller = Controller(STORAGE_LIMIT, kp=STO_KP, ki=STO_KI, kd=STO_KD)
        self.queue = []
        self.current_time = 0
        self.cleanup_task_id = 1

        # set memory controllers
        for cr in compute_resources:
            cr.set_mem_controller(memory_threshold=MEMORY_THRESHOLD, kp=MEM_KP, ki=MEM_KI, kd=MEM_KD)

    def start(self, enable_pid=True):
        """

        :param enable_pid: whether the PID controller is enabled
        """
        changed_schedule = True

        while not self.workflow.is_completed():

            # advance time step
            self.current_time += 1

            # process finished tasks
            finished_tasks = False
            for compute_resource in self.compute_resources:
                for compute_unit in compute_resource.compute_units.values():
                    if compute_unit.current_task and compute_unit.current_task.end_time <= self.current_time:
                        finished_task = compute_unit.current_task
                        compute_resource.process_finished_task(compute_unit)
                        del self.workflow.pending_tasks[finished_task.id]
                        print "[%s] Finished %s" % (self.current_time, finished_task)
                        finished_tasks = True

            # feed the PID controllers with current output values
            # TODO: only works for shared storage
            disk_controller_input = 0
            mem_controllers = {}
            if enable_pid and (changed_schedule or finished_tasks):
                disk_controller_input = self.disk_controller.process(self.shared_storage.current_used_storage())

                for cr in self.compute_resources:
                    mem_controllers[cr] = cr.get_mem_controller_input()

            mem_controller_response = False
            for cr in self.compute_resources:
                if cr in mem_controllers:
                    mem_controller_input = mem_controllers[cr]
                else:
                    mem_controller_input = 0.0
                if mem_controller_input != 0:
                    mem_controller_response = True
                mci = mem_controller_input
                if mci > cr.memory['capacity']:
                    mci = cr.memory['capacity']
                print "[%s] Mem Controller Input [%s]: %s - %s" % (self.current_time, cr.id, mci,
                                                                   cr.get_current_used_memory())

            dci = disk_controller_input
            if dci > STORAGE_CAPACITY:
                dci = STORAGE_CAPACITY
            print "[%s] Disk Controller Input: %s - %s" % (self.current_time, dci,
                                                           self.shared_storage.current_used_storage())

            if not finished_tasks and not changed_schedule:
                continue

            changed_schedule = False

            # add ready jobs to the queue
            for task in self.workflow.pending_tasks.values():
                if task.status == TaskStatus.IDLE and task not in self.queue and task.is_ready():
                    self.queue.append(task)
                    task.status = TaskStatus.QUEUED

            # tasks will be scheduled/preempted according to the controller information
            diff_input = disk_controller_input

            num_tasks_scheduled = 0
            num_tasks_preempted = 0

            # controllers indicate that more tasks may be scheduled
            if not enable_pid or disk_controller_input > 0:

                # associate tasks to compute units
                insufficient_space_error = False

                tasks_to_schedule = list(self.queue)

                while len(tasks_to_schedule) > 0:
                    task = random.choice(tasks_to_schedule)

                    # check if task estimation is on the limits of the input control
                    if enable_pid and task.type != TaskType.CLEANUP \
                            and STORAGE_ESTIMATION[task.transformation] > diff_input:
                        tasks_to_schedule.remove(task)
                        continue

                    try:
                        for compute_resource in self.compute_resources:
                            # test whether it has enough memory available (from estimation)
                            if enable_pid and task.type != TaskType.CLEANUP \
                                    and MEMORY_ESTIMATION[task.transformation] > mem_controllers[compute_resource]:
                                continue

                            compute_unit = compute_resource.run_task(task)
                            if compute_unit:
                                task.run(self.current_time)
                                self.queue.remove(task)
                                changed_schedule = True
                                num_tasks_scheduled += 1
                                if task.type != TaskType.CLEANUP:
                                    diff_input -= STORAGE_ESTIMATION[task.transformation]
                                    mem_controllers[compute_resource] -= MEMORY_ESTIMATION[task.transformation]
                                break

                        # TODO: only works for shared storage
                        if enable_pid and self.shared_storage.current_used_storage() > STORAGE_LIMIT:
                            break

                    except InsufficientSpace as e:
                        # add cleanup task if possible
                        insufficient_space_error = True
                    except InsufficientMemory as e:
                        # there is nothing to do, just wait for other tasks to finish
                        pass

                    tasks_to_schedule.remove(task)

                print "[%s] Tasks Scheduled: %s" % (self.current_time, num_tasks_scheduled)

                # create cleanup tasks if no tasks could be scheduled due to insufficient disk space
                if insufficient_space_error and not changed_schedule:
                    cleanup_task = self._create_cleanup_task()
                    if cleanup_task:
                        self.workflow.pending_tasks[cleanup_task.id] = cleanup_task

            # a controller is in overflow mode, thus tasks should be preempted
            elif disk_controller_input < 0:
                while diff_input < 0:
                    total_running_tasks = 0
                    latest_compute_resource = None
                    latest_started_task = None

                    for compute_resource in self.compute_resources:
                        running_tasks = compute_resource.get_running_tasks()
                        total_running_tasks += len(running_tasks)

                        if len(running_tasks) > 0:
                            for task in running_tasks:
                                if not latest_started_task or latest_started_task.start_time < task.start_time:
                                    latest_started_task = task
                                    latest_compute_resource = compute_resource

                    if total_running_tasks > 1:
                        # preempt the latest started task
                        preempted_task = latest_compute_resource.preempt_task(latest_started_task,
                                                                              self._get_required_files())
                        if preempted_task:
                            diff_input += STORAGE_ESTIMATION[preempted_task.transformation]
                            changed_schedule = True
                            num_tasks_preempted += 1
                            print "[PREEMPTED] %s" % preempted_task
                    else:
                        break

                print "[%s] Tasks Preempted: %s" % (self.current_time, num_tasks_preempted)

            print "[Time] %s\n%s" % (self.current_time, print_dictionary_ids(self.compute_resources))

        print "\nWorkflow Makespan: %s\n" % self.current_time

    def _create_cleanup_task(self):
        """
        Create cleanup task to removed unused (and not required) data from disk.
        :return: cleanup task object
        """
        current_used_files = []
        for compute_resource in self.compute_resources:
            current_used_files.extend(compute_resource.get_list_of_current_used_files())

        required_files = self._get_required_files()
        for f in current_used_files:
            if f not in required_files:
                required_files.append(f)

        task_id = "cleanup_%s" % self.cleanup_task_id
        cleanup_task = Task(task_id, 0, type=TaskType.CLEANUP)

        total_size = 0
        for f in self.shared_storage.files.values():
            if f not in required_files:
                cleanup_task.input_data[f.name] = f
                total_size += f.size

        if total_size == 0:
            return None

        cleanup_task.duration = total_size * 10
        self.cleanup_task_id += 1
        # print cleanup_task
        return cleanup_task

    def _get_required_files(self):
        """
        Get list of required files by pending tasks.
        :return: list of required files
        """
        required_files = []
        for task in self.workflow.pending_tasks.values():
            for f in task.input_data.values():
                if f not in required_files:
                    required_files.append(f)

        return required_files
