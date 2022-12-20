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
from datasketches import density_doubles_sketch
import numpy as np

class densityTest(unittest.TestCase):
  def test_density_sketch(self):
    k = 10
    dim = 3
    n = 1000

    sketch = density_doubles_sketch(k, dim)

    self.assertEqual(sketch.get_k(), k)
    self.assertEqual(sketch.get_dim(), dim)
    self.assertTrue(sketch.is_empty())
    self.assertFalse(sketch.is_estimation_mode())
    self.assertEqual(sketch.get_n(), 0)
    self.assertEqual(sketch.get_num_retained(), 0)

    for i in range(n):
      sketch.update([i, i, i])

    self.assertFalse(sketch.is_empty())
    self.assertTrue(sketch.is_estimation_mode())
    self.assertEqual(sketch.get_n(), n)
    self.assertGreater(sketch.get_num_retained(), k)
    self.assertLess(sketch.get_num_retained(), n)
    self.assertGreater(sketch.get_estimate([n - 1, n - 1, n - 1]), 0)

    print(sketch.to_string())

    list = sketch.get_coreset()
    self.assertEqual(len(list), sketch.get_num_retained())

  def test_density_merge(self):
    sketch1 = density_doubles_sketch(10, 2)
    sketch1.update([0, 0])
    sketch2 = density_doubles_sketch(10, 2)
    sketch2.update([0, 1])
    sketch1.merge(sketch2)
    self.assertEqual(sketch1.get_n(), 2)
    self.assertEqual(sketch1.get_num_retained(), 2)

if __name__ == '__main__':
    unittest.main()
