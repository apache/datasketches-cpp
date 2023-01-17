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

from .TuplePolicy import TuplePolicy
from _datasketches import _update_tuple_sketch, _tuple_union, _tuple_intersection
from _datasketches import _tuple_a_not_b#, _tuple_jaccard_similarity
from _datasketches import tuple_sketch, compact_tuple_sketch

class update_tuple_sketch(tuple_sketch):
  def __init__(self, policy, lg_k:int = 12, p:float = 1.0, seed:int = 9001):
    #tuple_sketch.__init__(self) # abstract so a no-op
    self._policy = policy
    self._gadget = _update_tuple_sketch(self._policy, lg_k, p, seed)

  # TODO: do we need multiple update formats?
  def update(self, datum, summary):
    self._gadget.update(datum, summary)

  def compact(self, ordered:bool = True) -> compact_tuple_sketch:
    return self._gadget.compact(ordered)

class tuple_union:
  # TODO: define seed from constant
  def __init__(self, policy:TuplePolicy, lg_k:int = 12, p:float = 1.0, seed:int = 9001):
    self._policy = policy
    self._gadget = _tuple_union(self._policy, lg_k, p, seed)

  def update(self, sketch:tuple_sketch):
    if isinstance(sketch, compact_tuple_sketch):
      self._gadget.update(sketch)
    else:
      self._gadget.update(sketch._gadget)

  def get_result(self, ordered:bool = True) -> compact_tuple_sketch:
    return self._gadget.get_result()

  def reset(self):
    self._gadget.reset()

class tuple_intersection:
  # TODO: define seed from constant
  def __init__(self, policy:TuplePolicy, seed:int = 9001):
    self._policy = policy
    self._gadget = _tuple_intersection(self._policy, seed)

  def update(self, sketch:tuple_sketch):
    if isinstance(sketch, compact_tuple_sketch):
      self._gadget.update(sketch)
    else:
      self._gadget.update(sketch._gadget)

  def has_result(self) -> bool:
    return self._gadget.has_result()

  def get_result(self, ordered:bool = True) -> compact_tuple_sketch:
    return self._gadget.get_result(ordered)

class tuple_a_not_b:
  def __init__(self, seed:int = 9001):
    self._gadget = _tuple_a_not_b(seed)
  
  def compute(self, a:tuple_sketch, b:tuple_sketch, ordered:bool=True) -> compact_tuple_sketch:
    return self._gadget.compute(a, b)
