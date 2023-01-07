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

from _datasketches import TuplePolicy

# This file provides an example Python Tuple Policy implementation.
#
# Each implementation must extend the PyTuplePolicy class and define
# two methods:
#   * create() returns a new Summary object
#   * update(summary, update) applies the relevant policy to update the
#     provided summary with the data in update.

# Implements an accumulator summary policy, where new values are
# added to the existing value.
class AccumulatorUpdatePolicy(TuplePolicy):
  def __init__(self):
    TuplePolicy.__init__(self)

  def create_summary(self):
    return int(0)

  def update_summary(self, summary: int, update: int):
    summary += update
    return summary
