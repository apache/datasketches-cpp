# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import numpy as np

from _datasketches import KernelFunction

# This file provides an example Python Kernel Function implementation.
#
# Each implementation must extend the KernelFunction class
# and define the __call__ method

# Implements a basic Gaussian Kernel
class GaussianKernel(KernelFunction):
  def __init__(self, bandwidth: float=1.0):
    KernelFunction.__init__(self)
    self._bw = bandwidth
    self._scale = -0.5 * (bandwidth ** -2)

  def __call__(self, a: np.array, b: np.array) -> float:
    return np.exp(self._scale * np.linalg.norm(a - b)**2)
