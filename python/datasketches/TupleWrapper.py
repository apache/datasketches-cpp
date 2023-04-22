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

from _datasketches import _tuple_sketch, _compact_tuple_sketch, _update_tuple_sketch
from _datasketches import  _tuple_union, _tuple_intersection
from _datasketches import _tuple_a_not_b, _tuple_jaccard_similarity
from _datasketches import PyObjectSerDe, theta_sketch, TuplePolicy

class tuple_sketch(ABC):
  """An abstract base class representing a Tuple Sketch."""
  _gadget: _tuple_sketch

  def __str__(self, print_items:bool=False):
    return self._gadget.to_string(print_items)

  def is_empty(self):
    """Returns True if the sketch is empty, otherwise False."""
    return self._gadget.is_empty()

  def get_estimate(self):
    """Returns an estimate of the distinct count of the input stream."""
    return self._gadget.get_estimate()

  def get_upper_bound(self, num_std_devs:int):
    """Returns an approximate upper bound on the estimate at standard deviations in {1, 2, 3}."""
    return self._gadget.get_upper_bound(num_std_devs)

  def get_lower_bound(self, num_std_devs:int):
    """Returns an approximate lower bound on the estimate at standard deviations in {1, 2, 3}."""
    return self._gadget.get_lower_bound(num_std_devs)

  def is_estimation_mode(self):
    """Returns True if the sketch is in estimation mode, otherwise False."""
    return self._gadget.is_estimation_mode()

  def get_theta(self):
    """Returns theta (the effective sampling rate) as a fraction from 0 to 1."""
    return self._gadget.get_theta()

  def get_theta64(self):
    """Returns theta as a 64-bit integer value."""
    return self._gadget.get_theta64()

  def get_num_retained(self):
    """Returns the number of items currently in the sketch."""
    return self._gadget.get_num_retained()

  def get_seed_hash(self):
    """Returns a hash of the seed used in the sketch."""
    return self._gadget.get_seed_hash()

  def is_ordered(self):
    """Returns True if the sketch entries are sorder, otherwise False."""
    return self._gadget.is_ordered()

  def __iter__(self):
    return self._gadget.__iter__()


class compact_tuple_sketch(tuple_sketch):
  """An instance of a Tuple Sketch that has been compacted and can no longer accept updates."""

  def __init__(self, other:tuple_sketch, ordered:bool = True):
    if other == None:
      self._gadget = None
    else:
      self._gadget = _compact_tuple_sketch(other, ordered)

  def serialize(self, serde:PyObjectSerDe):
    """Serializes the sketch into a bytes object with the provided SerDe."""
    return self._gadget.serialize(serde)

  @classmethod
  def from_theta_sketch(cls, sketch:theta_sketch, summary, seed:int=_tuple_sketch.DEFAULT_SEED):
    """Creates a comapct Tuple Sketch from a Theta Sketch using a fixed summary value."""
    self = cls.__new__(cls)
    self._gadget = _compact_tuple_sketch(sketch, summary, seed)
    return self

  @classmethod
  def deserialize(cls, data:bytes, serde:PyObjectSerDe, seed:int=_tuple_sketch.DEFAULT_SEED):
    """Reads a bytes object and uses the provded SerDe to return the corresponding compact_tuple_sketch."""
    self = cls.__new__(cls)
    self._gadget = _compact_tuple_sketch.deserialize(data, serde, seed)
    return self


class update_tuple_sketch(tuple_sketch):
  """An instance of a Tuple Sketch that is available for updates. Requires a Policy object to handle Summary values."""

  def __init__(self, policy, lg_k:int = 12, p:float = 1.0, seed:int = _tuple_sketch.DEFAULT_SEED):
    self._policy = policy
    self._gadget = _update_tuple_sketch(self._policy, lg_k, p, seed)

  def update(self, datum, value):
    """Updates the sketch with the provided item and summary value."""
    self._gadget.update(datum, value)

  def compact(self, ordered:bool = True) -> compact_tuple_sketch:
    """Returns a compacted form of the sketch, optionally sorting it."""
    return self._gadget.compact(ordered)

  def reset(self):
    """Resets the sketch to the initial empty state."""
    self._gadget.reset()


class tuple_union:
  """An object that can merge Tuple Sketches. Requires a Policy object to handle merging Summaries."""
  _policy: TuplePolicy

  def __init__(self, policy:TuplePolicy, lg_k:int = 12, p:float = 1.0, seed:int = _tuple_sketch.DEFAULT_SEED):
    self._policy = policy
    self._gadget = _tuple_union(self._policy, lg_k, p, seed)

  def update(self, sketch:tuple_sketch):
    """Updates the union with the given sketch."""
    self._gadget.update(sketch._gadget)

  def get_result(self, ordered:bool = True) -> compact_tuple_sketch:
    """Returns the sketch corresponding to the union result, optionally sorted."""
    return compact_tuple_sketch(self._gadget.get_result(ordered), ordered)

  def reset(self):
    """Resets the union to the initial empty state."""
    self._gadget.reset()


class tuple_intersection:
  """An object that can intersect Tuple Sketches. Requires a Policy object to handle intersecting Summaries."""
  _policy: TuplePolicy

  def __init__(self, policy:TuplePolicy, seed:int = _tuple_sketch.DEFAULT_SEED):
    self._policy = policy
    self._gadget = _tuple_intersection(self._policy, seed)

  def update(self, sketch:tuple_sketch):
    """Intersects the provided sketch with the current intersection state."""
    self._gadget.update(sketch._gadget)

  def has_result(self) -> bool:
    """Returns True if the intersection has a valid result, otherwise False."""
    return self._gadget.has_result()

  def get_result(self, ordered:bool = True) -> compact_tuple_sketch:
    """Returns the sketch corresponding to the intersection result, optionally sorted."""
    return compact_tuple_sketch(self._gadget.get_result(ordered), ordered)


class tuple_a_not_b:
  """An object that can peform the A-not-B operation between two sketches."""
  def __init__(self, seed:int = _tuple_sketch.DEFAULT_SEED):
    self._gadget = _tuple_a_not_b(seed)
  
  def compute(self, a:tuple_sketch, b:tuple_sketch, ordered:bool=True) -> compact_tuple_sketch:
    """Returns a sketch with the result of applying the A-not-B operation on the given inputs."""
    return compact_tuple_sketch(self._gadget.compute(a._gadget, b._gadget))


class tuple_jaccard_similarity:
  @staticmethod  
  def jaccard(a:tuple_sketch, b:tuple_sketch, seed:int=_tuple_sketch.DEFAULT_SEED):
    """Returns a list with {lower_bound, estimate, upper_bound} of the Jaccard similarity between sketches."""
    return _tuple_jaccard_similarity.jaccard(a._gadget, b._gadget, seed)

  @staticmethod
  def exactly_equal(a:tuple_sketch, b:tuple_sketch, seed:int=_tuple_sketch.DEFAULT_SEED):
    """Returns True if sketch_a and sketch_b are equivalent, otherwise False."""
    return _tuple_jaccard_similarity.exactly_equal(a._gadget, b._gadget, seed)

  @staticmethod
  def similarity_test(actual:tuple_sketch, expected:tuple_sketch, threshold:float, seed:int=_tuple_sketch.DEFAULT_SEED):
    """Tests similarity of an actual sketch against an expected sketch.

    Computes the lower bound of the Jaccard index J_{LB} of the actual and expected sketches.
    If J_{LB} >= threshold, then the sketches are considered to be similar sith a confidence of
    97.7% and returns True, otherwise False.
    """
    return _tuple_jaccard_similarity.similarity_test(actual._gadget, expected._gadget, threshold, seed)

  @staticmethod
  def dissimilarity_test(actual:tuple_sketch, expected:tuple_sketch, threshold:float, seed:int=_tuple_sketch.DEFAULT_SEED):
    """Tests dissimilarity of an actual sketch against an expected sketch.

    Computes the upper bound of the Jaccard index J_{UB} of the actual and expected sketches.
    If J_{UB} <= threshold, then the sketches are considered to be dissimilar sith a confidence of
    97.7% and returns True, otherwise False.
    """
    return _tuple_jaccard_similarity.dissimilarity_test(actual._gadget, expected._gadget, threshold, seed)
