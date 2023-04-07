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

import sys

from _datasketches import TuplePolicy

# This file provides an example Python Tuple Policy implementation.
#
# Each implementation must extend the PyTuplePolicy class and define
# two methods:
#   * create_summary() returns a new Summary object
#   * update_summary(summary, update) applies the relevant policy to update the
#     provided summary with the data in update.
#   * __call__ may be similar to update_summary but allows a different
#     implementation for set operations (union and intersection)

# Implements an accumulator summary policy, where new values are
# added to the existing value.
class AccumulatorPolicy(TuplePolicy):
  def __init__(self):
    TuplePolicy.__init__(self)

  def create_summary(self) -> int:
    return int(0)

  def update_summary(self, summary: int, update: int) -> int:
    summary += update
    return summary

  def __call__(self, summary: int, update: int) -> int:
    summary += update
    return summary


# Implements a MAX rule, where the largest integer value is always kept
class MaxIntPolicy(TuplePolicy):
  def __init__(self):
    TuplePolicy.__init__(self)

  def create_summary(self) -> int:
    return int(-sys.maxsize-1)

  def update_summary(self, summary: int, update: int) -> int:
    return max(summary, update)

  def __call__(self, summary: int, update: int) -> int:
    return max(summary, update)


# Implements a MIN rule, where the smallest integer value is always kept
class MinIntPolicy(TuplePolicy):
  def __init__(self):
    TuplePolicy.__init__(self)

  def create_summary(self) -> int:
    return int(sys.maxsize)

  def update_summary(self, summary: int, update: int) -> int:
    return min(summary, update)

  def __call__(self, summary: int, update: int) -> int:
    return min(summary, update)
