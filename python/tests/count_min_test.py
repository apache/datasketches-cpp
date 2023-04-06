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
  
import unittest
from datasketches import count_min_sketch

class CountMinTest(unittest.TestCase):
  def test_count_min_example(self):
    # we'll define target confidence and relative error and use the built-in
    # methods to determine how many hashes and buckets to use
    confidence = 0.95
    num_hashes = count_min_sketch.suggest_num_hashes(confidence)
    relative_error = 0.01
    num_buckets = count_min_sketch.suggest_num_buckets(relative_error)

    # now we can create a few empty sketches
    cm = count_min_sketch(num_hashes, num_buckets)
    cm2 = count_min_sketch(num_hashes, num_buckets)
    self.assertTrue(cm.is_empty())

    # we'll use a moderate number of distinct items with
    # increasing weights, with each item's weight being
    # equal to its value
    n = 1000
    total_wt = 0
    for i in range(1, n+1):
      cm.update(i, i)
      total_wt += i
    self.assertFalse(cm.is_empty())
    self.assertEqual(cm.get_total_weight(), total_wt)

    # querying the items, each of them should
    # have a non-zero count.  the estimate should
    # be at least i with appropriately behaved bounds.
    for i in range(1, n+1):
      val = cm.get_estimate(i)
      self.assertGreaterEqual(val, i)
      self.assertGreaterEqual(val, cm.get_lower_bound(i))
      self.assertGreater(cm.get_upper_bound(i), val)

    # values not in the sketch should have lower estimates, but
    # are not guaranteed to be zero and will succeed
    self.assertIsNotNone(cm.get_estimate("not in set"))

    # we can create another sketch with partial overlap
    # and merge them
    for i in range(int(n / 2), int(3 * n / 2)):
      cm2.update(i, i)
    cm.merge(cm2)

    # and the estimated weight for the overlapped meerged values
    # (n/2 to n) should now be at least 2x the value
    self.assertGreaterEqual(cm.get_estimate(n), 2 * n)

    # finally, serialize and reconstruct
    cm_bytes = cm.serialize()
    self.assertEqual(cm.get_serialized_size_bytes(), len(cm_bytes))
    new_cm = count_min_sketch.deserialize(cm_bytes)

    # and now interrogate the sketch
    self.assertFalse(new_cm.is_empty())
    self.assertEqual(new_cm.get_num_hashes(), cm.get_num_hashes())
    self.assertEqual(new_cm.get_num_buckets(), cm.get_num_buckets())
    self.assertEqual(new_cm.get_total_weight(), cm.get_total_weight())
    
    # we can also iterate through values in and out of the sketch to ensure
    # the estimates match
    for i in range(0, 2 * n):
      self.assertEqual(cm.get_estimate(i), new_cm.get_estimate(i))

if __name__ == '__main__':
    unittest.main()
