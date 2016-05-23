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

from file import FileLink
from util import *

log = logging.getLogger(__name__)


class Element:
    TASK = "task"
    FILE = "file"
    USES = "uses"
    DEPENDS = "depends"


class Workflow:
    def __init__(self):
        self.tasks = {}
        self.pending_tasks = {}
        self.files = {}

    def add_task(self, task):
        """

        :param task:
        :return:
        """
        self.tasks[task.id] = task
        self.pending_tasks[task.id] = task

    def add_file(self, file):
        """

        :param file:
        :return:
        """
        if file.name not in self.files:
            self.files[file.name] = file

    def add_use(self, task_id, file_name, link):
        """

        :param task_id:
        :param file_name:
        :param link:
        :return:
        """
        task = self.tasks[task_id]
        file = self.files[file_name]

        if link == FileLink.INPUT:
            task.input_data[file.name] = file
        elif link == FileLink.OUTPUT:
            task.output_data[file.name] = file
        elif link == FileLink.INTERMEDIATE:
            task.intermediate_data[file.name] = file

    def add_dependency(self, child_id, parent_id):
        """

        :param child_id:
        :param parent_id:
        :return:
        """
        child_task = self.tasks[child_id]
        child_task.add_parent(self.tasks[parent_id])

    def is_completed(self):
        return len(self.pending_tasks) == 0

    def __str__(self):
        """

        :return:
        """
        out_str = "Workflow {\n"
        # tasks
        out_str += "  tasks:\n"
        for task in self.tasks.values():
            out_str += "    %s\n" % task

        # files
        out_str += "  files:\n"
        for file in self.files.values():
            out_str += "    %s\n" % file

        # pending tasks
        out_str += "  pending_tasks: \n" \
                   "    (%s)\n" % print_dictionary_ids(self.pending_tasks)

        out_str += "}"
        return out_str
