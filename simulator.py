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
from workflow import *
from resource import *
from pid_scheduler import PIDScheduler

log = logging.getLogger(__name__)


def main():
    args = sys.argv[1:]

    wf = Workflow()

    wf_file = open(args[0])
    use_pid = True

    if len(args) > 1 and "--no-pid" in args:
        use_pid = False

    for line in wf_file:
        l = line.strip()
        if len(l.strip()) > 0 and not l.startswith("#"):
            v = l.lower().split(",")
            element_type = str(v[0])

            if element_type == Element.TASK:
                wf.add_task(Task(v[1], v[2], v[3]))

            elif element_type == Element.FILE:
                wf.add_file(File(v[1], v[2]))

            elif element_type == Element.USES:
                wf.add_use(v[1], v[2], v[3])

            elif element_type == Element.DEPENDS:
                wf.add_dependency(v[1], v[2])

    # shared storage
    shared_storage = Storage(500000)

    # compute resources
    # Large cluster, 2TB RAM, 32 cores
    cr_large = ComputeResource("cluster-large", accepted_tasks=[TaskTransformation.INDIVIDUALS],
                               shared_storage=shared_storage, memory_capacity=2000000)
    cr_large.generate_compute_units(compute_units=32)

    # Intermediate cluster, 192GB RAM, 16 cores
    cr_inter = ComputeResource("cluster-intermediate", accepted_tasks=[TaskTransformation.SIFTING],
                               shared_storage=shared_storage, memory_capacity=192000)
    cr_inter.generate_compute_units(compute_units=16)

    # Small cluster, 64GB RAM, 32 cores
    cr_small = ComputeResource("cluster-small", accepted_tasks=[TaskTransformation.POPULATION, TaskTransformation.PAIR,
                                                                TaskTransformation.FREQUENCY],
                               shared_storage=shared_storage, memory_capacity=100000)
    cr_small.generate_compute_units(compute_units=32)

    compute_resources = [cr_large, cr_inter, cr_small]

    # create scheduler and start simulation
    pid_scheduler = PIDScheduler(wf, compute_resources, shared_storage)
    pid_scheduler.start(enable_pid=use_pid)


if __name__ == '__main__':
    main()
