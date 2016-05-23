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

from abc import ABCMeta, abstractmethod

log = logging.getLogger(__name__)


class Controller:
    def __init__(self, setpoint, kp=1, ki=1, kd=1, error=0.05):
        self.setpoint = setpoint
        self.kp = kp
        self.ki = ki
        self.kd = kd
        self.error = error
        self.cumulative_error = 0.0
        self.previous_error = 0.0

    def process(self, output_value):
        # calculate error
        error = self.setpoint - float(output_value)

        if abs(error) < 0 + (self.setpoint * self.error):
            self.previous_error = error
            self.cumulative_error = 0
            return 0

        # PID
        controller_input = self.kp * error + self.ki * self.cumulative_error + self.kd * self.previous_error

        # update errors
        self.cumulative_error += error
        self.previous_error = error

        return controller_input
