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

import logging

from util import *

log = logging.getLogger(__name__)


class TaskStatus:
    IDLE = "idle"
    READY = "ready"
    QUEUED = "queued"
    RUNNING = "running"
    FAILED = "failed"
    COMPLETED = "completed"


class TaskType:
    REGULAR = "regular"
    CLEANUP = "cleanup"


class TaskTransformation:
    SIFTING = "sifting"
    INDIVIDUALS = "individuals"
    POPULATION = "population"
    PAIR = "pair"
    FREQUENCY = "frequency"


class Task:
    def __init__(self, id, duration, peak_memory=0.0, type=TaskType.REGULAR):
        self.id = id
        self.transformation = id.split('_')[0]
        self.duration = float(duration)
        self.peak_memory = int(peak_memory)
        self.input_data = {}
        self.intermediate_data = {}
        self.output_data = {}
        self.parent_tasks = {}
        self.status = TaskStatus.IDLE
        self.start_time = -1
        self.end_time = -1
        self.type = type

    def add_parent(self, parent_task):
        self.parent_tasks[parent_task.id] = parent_task

    def is_ready(self):
        for task in self.parent_tasks.values():
            if task.status != TaskStatus.COMPLETED:
                return False
        return True

    def run(self, start_time):
        self.start_time = start_time
        self.status = TaskStatus.RUNNING
        self.end_time = self.start_time + self.duration

    def preempt(self):
        self.status = TaskStatus.IDLE
        self.start_time = -1
        self.end_time = -1

    def __str__(self):
        input_data = print_dictionary_ids(self.input_data)
        intermediate_data = print_dictionary_ids(self.intermediate_data)
        output_data = print_dictionary_ids(self.output_data)
        parent_tasks = print_dictionary_ids(self.parent_tasks)

        return "Task: {id: %s, duration: %s, peak_memory: %s, status: %s, type: %s, parent_tasks: (%s), " \
               "input_data: (%s), intermediate_data: (%s), output_data: (%s)}" \
               % (self.id, self.duration, self.peak_memory, self.status, self.type, parent_tasks, input_data,
                  intermediate_data, output_data)
