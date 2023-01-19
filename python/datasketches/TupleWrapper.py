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

from abc import ABC, abstractmethod

from .TuplePolicy import TuplePolicy
from _datasketches import _tuple_sketch, _compact_tuple_sketch, _update_tuple_sketch
from _datasketches import  _tuple_union, _tuple_intersection
from _datasketches import _tuple_a_not_b, _tuple_jaccard_similarity
from _datasketches import PyObjectSerDe

class tuple_sketch(ABC):
  _gadget: _tuple_sketch

  def __str__(self, print_items:bool=False):
    return self._gadget.to_string(print_items)

  def is_empty(self):
    return self._gadget.is_empty()

  def get_estimate(self):
    return self._gadget.get_estimate()

  def get_upper_bound(self, num_std_devs:int):
    return self._gadget.get_upper_bound(num_std_devs)

  def get_lower_bound(self, num_std_devs:int):
    return self._gadget.get_lower_bound(num_std_devs)

  def is_estimation_mode(self):
    return self._gadget.is_estimation_mode()

  def get_theta(self):
    return self._gadget.get_theta()

  def get_theta64(self):
    return self._gadget.get_theta64()

  def get_num_retained(self):
    return self._gadget.get_num_retained()

  def get_seed_hash(self):
    return self._gadget.get_seed_hash()

  def is_ordered(self):
    return self._gadget.is_ordered()

  def __iter__(self):
    return self._gadget.__iter__()

  #.def("__iter__", [](const py_tuple_sketch& s) { return py::make_iterator(s.begin(), s.end()); })


class compact_tuple_sketch(tuple_sketch):
  def __init__(self, other:tuple_sketch, ordered:bool = True):
    if other == None:
      self._gadget = None
    else:
      self._gadget = _compact_tuple_sketch(other, ordered)

  def serialize(self, serde:PyObjectSerDe):
    return self._gadget.serialize(serde)

  # TODO: define seed from constant
  @staticmethod
  def deserialize(data:bytes, serde:PyObjectSerDe, seed:int=9001):
    cpp_sk = _compact_tuple_sketch.deserialize(data, serde, seed)
    # TODO: this seems inefficinet -- is there some sort of _wrap()
    # approach that might work better?
    sk = compact_tuple_sketch(None, True)
    sk._gadget = cpp_sk
    return sk


class update_tuple_sketch(tuple_sketch):
  # TODO: define seed from constant
  def __init__(self, policy, lg_k:int = 12, p:float = 1.0, seed:int = 9001):
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
      self._gadget.update(sketch._gadget)

  def get_result(self, ordered:bool = True) -> compact_tuple_sketch:
    sk = compact_tuple_sketch(self._gadget.get_result(ordered), ordered)
    return sk

  def reset(self):
    self._gadget.reset()


class tuple_intersection:
  # TODO: define seed from constant
  def __init__(self, policy:TuplePolicy, seed:int = 9001):
    self._policy = policy
    self._gadget = _tuple_intersection(self._policy, seed)

  def update(self, sketch:tuple_sketch):
    self._gadget.update(sketch._gadget)

  def has_result(self) -> bool:
    return self._gadget.has_result()

  def get_result(self, ordered:bool = True) -> compact_tuple_sketch:
    sk = compact_tuple_sketch(self._gadget.get_result(ordered), ordered)
    return sk


class tuple_a_not_b:
  def __init__(self, seed:int = 9001):
    self._gadget = _tuple_a_not_b(seed)
  
  def compute(self, a:tuple_sketch, b:tuple_sketch, ordered:bool=True) -> compact_tuple_sketch:
    sk = compact_tuple_sketch(self._gadget.compute(a._gadget, b._gadget))
    return sk


class tuple_jaccard_similarity:
  # TODO: define seed from constant
  @staticmethod  
  def jaccard(a:tuple_sketch, b:tuple_sketch, seed:int=9001):
    return _tuple_jaccard_similarity.jaccard(a._gadget, b._gadget, seed)

  @staticmethod
  def exactly_equal(a:tuple_sketch, b:tuple_sketch, seed:int=9001):
    return _tuple_jaccard_similarity.exactly_equal(a._gadget, b._gadget, seed)

  @staticmethod
  def similarity_test(actual:tuple_sketch, expected:tuple_sketch, threshold:float, seed:int=9001):
    return _tuple_jaccard_similarity.similarity_test(actual._gadget, expected._gadget, threshold, seed)

  @staticmethod
  def dissimilarity_test(actual:tuple_sketch, expected:tuple_sketch, threshold:float, seed:int=9001):
    return _tuple_jaccard_similarity.dissimilarity_test(actual._gadget, expected._gadget, threshold, seed)
