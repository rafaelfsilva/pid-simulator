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

import sys

from file import *
from task import TaskTransformation


def parse_task_transformation(value):
    if value.lower() == "sifting":
        return TaskTransformation.SIFTING
    elif value.lower() == "individuals":
        return TaskTransformation.INDIVIDUALS
    elif value.lower() == "population":
        return TaskTransformation.POPULATION
    elif value.lower() == "pair":
        return TaskTransformation.PAIR
    elif value.lower() == "frequency":
        return TaskTransformation.FREQUENCY

    return None


class Task:
    def __init__(self, type, id, duration, memory_peak):
        self.type = type
        self.id = id
        self.duration = float(duration)
        self.memory_peak = int(memory_peak)
        self.input_files = []
        self.intermediate_files = []
        self.output_files = []
        self.parents = []

    def __str__(self):
        return "Task,%s_%s,%s,%s" % (self.type, self.id, self.duration, self.memory_peak)


def main():
    args = sys.argv[1:]

    # read and parse workflow csv file
    wf_file = open(args[0])

    tasks = []
    files = []

    for line in wf_file:
        l = line.strip().split(",")
        task = Task(parse_task_transformation(l[0]), l[1], l[2], l[3])

        if task.type == TaskTransformation.SIFTING or task.type == TaskTransformation.INDIVIDUALS \
                or task.type == TaskTransformation.POPULATION:
            input_file = File("input_%s_%s" % (l[0], l[1]), l[4])
            files.append(input_file)
            task.input_files.append(input_file)

        elif task.type == TaskTransformation.PAIR or task.type == TaskTransformation.FREQUENCY:
            multiplier = int((int(task.id) - 1) / 22)
            index = int(task.id) - multiplier * 22
            task.input_files.append(find_file("output_%s_%s" % (TaskTransformation.INDIVIDUALS, index), files))
            task.input_files.append(find_file("output_%s_%s" % (TaskTransformation.SIFTING, index), files))
            task.input_files.append(find_file("output_%s_%s" % (TaskTransformation.POPULATION, multiplier + 1), files))
            task.parents.append(find_task(TaskTransformation.INDIVIDUALS, index, tasks))
            task.parents.append(find_task(TaskTransformation.SIFTING, index, tasks))
            task.parents.append(find_task(TaskTransformation.POPULATION, multiplier + 1, tasks))

        if float(l[5]) > 0:
            intermediate_file = File("intermediate_%s_%s" % (l[0], l[1]), l[5])
            files.append(intermediate_file)
            task.intermediate_files.append(intermediate_file)

        if float(l[6]) > 0:
            output_file = File("output_%s_%s" % (l[0], l[1]), l[6])
            files.append(output_file)
            task.output_files.append(output_file)

        tasks.append(task)

    # print the workflow
    for file in files:
        print "File,%s,%s" % (file.name, file.size)

    for task in tasks:
        print task
        task_name = "%s_%s" % (task.type, task.id)
        print_uses(task_name, task.input_files, FileLink.INPUT)
        print_uses(task_name, task.intermediate_files, FileLink.INTERMEDIATE)
        print_uses(task_name, task.output_files, FileLink.OUTPUT)

        for parent in task.parents:
            parent_name = "%s_%s" % (parent.type, parent.id)
            print "Depends,%s,%s" % (task_name, parent_name)


def find_file(file_name, files_list):
    for file in files_list:
        if file.name == file_name:
            return file
    return None


def find_task(task_type, task_id, tasks_list):
    for task in tasks_list:
        if task.type == task_type and int(task.id) == task_id:
            return task
    return None


def print_uses(task_id, files_list, link):
    for file in files_list:
        print "Uses,%s,%s,%s" % (task_id, file.name, link)


if __name__ == '__main__':
    main()
