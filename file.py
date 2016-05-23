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

log = logging.getLogger(__name__)


class FileLink:
    INPUT = "input"
    OUTPUT = "output"
    INTERMEDIATE = "intermediate"


class File:
    def __init__(self, name, size):
        self.name = name
        self.size = float(size)

    def __eq__(self, other):
        return self.name == other.name and self.size == other.size

    def __str__(self):
        return "File: {name: %s, size: %s}" % (self.name, self.size)
