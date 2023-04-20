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

from _datasketches import _density_sketch, KernelFunction
from .KernelFunction import GaussianKernel

class density_sketch:
  """An instance of a Density Sketch for kernel density estimation. Requires a KernelFunction object."""

  def __init__(self, k:int, dim:int, kernel:KernelFunction=GaussianKernel()):
    self._kernel = kernel
    self._gadget = _density_sketch(k, dim, self._kernel)

  @classmethod
  def deserialize(cls, data:bytes, kernel:KernelFunction=GaussianKernel()):
    """Reads a bytes object and returns a density sketch, using the provided kerenl or defaulting to a Guassian kerenl"""
    self = cls.__new__(cls)
    self._kernel = kernel
    self._gadget = _density_sketch.deserialize(data, kernel)
    return self

  def update(self, point:np.array):
    """Updates the sketch with the given point"""
    self._gadget.update(point)

  def merge(self, other:'density_sketch'):
    """Merges the provided sketch into this one"""
    self._gadget.merge(other._gadget)

  def is_empty(self):
    """Returns True if the sketch is empty, otherwise False"""
    return self._gadget.is_empty()

  def get_k(self):
    """Returns the configured parameter k"""
    return self._gadget.get_k()

  def get_dim(self):
    """Returns the configured parameter dim"""
    return self._gadget.get_dim()

  def get_n(self):
    """Returns the length of the input stream"""
    return self._gadget.get_n()

  def get_num_retained(self):
    """Returns the number of retained items (samples) in the sketch"""
    return self._gadget.get_num_retained()

  def is_estimation_mode(self):
    """Returns True if the sketch is in estimation mode, otherwise False"""
    return self._gadget.is_estimation_mode()

  def get_estimate(self, point:np.array):
    """Returns an approximate density at the given point"""
    return self._gadget.get_estimate(point)

  def serialize(self):
    """Serializes the sketch into a bytes object"""
    return self._gadget.serialize()

  def __str__(self, print_levels:bool=False, print_items:bool=False):
    """Produces a string summary of the sketch"""
    return self._gadget.to_string(print_levels, print_items)

  def to_string(self, print_levels:bool=False, print_items:bool=False):
    """Produces a string summary of the sketch"""
    return self._gadget.to_string(print_levels, print_items)

  def __iter__(self):
    return self._gadget.__iter__()
