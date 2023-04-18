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
from datasketches import density_sketch, KernelFunction
import numpy as np

class UnitSphereKernel(KernelFunction):
  def __call__(self, a: np.array, b: np.array) -> float:
    if np.linalg.norm(a - b) < 1.0:
      return 1.0
    else:
      return 0.0

class densityTest(unittest.TestCase):
  def test_density_sketch(self):
    k = 10
    dim = 3
    n = 1000

    sketch = density_sketch(k, dim)

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

    for tuple in sketch:
      vector = tuple[0]
      weight = tuple[1]
      self.assertEqual(len(vector), dim)
      self.assertGreaterEqual(weight, 1)

    sk_bytes = sketch.serialize()
    sketch2 = density_sketch.deserialize(sk_bytes)
    self.assertEqual(sketch.get_estimate([1.5, 2.5, 3.5]), sketch2.get_estimate([1.5, 2.5, 3.5]))

  def test_density_merge(self):
    sketch1 = density_sketch(10, 2)
    sketch1.update([0, 0])
    sketch2 = density_sketch(10, 2)
    sketch2.update([0, 1])
    sketch1.merge(sketch2)
    self.assertEqual(sketch1.get_n(), 2)
    self.assertEqual(sketch1.get_num_retained(), 2)

  def test_custom_kernel(self):
    gaussianSketch = density_sketch(10, 2) # default kernel
    sphericalSketch = density_sketch(10, 2, UnitSphereKernel())

    p = [1, 1]
    gaussianSketch.update(p)
    sphericalSketch.update(p)

    # Spherical kernel should return 1.0 for a nearby point, 0 farther
    # Gaussian kernel should return something nonzero when farther away
    self.assertEqual(sphericalSketch.get_estimate([1.001, 1]), 1.0)
    self.assertEqual(sphericalSketch.get_estimate([2, 2]), 0.0)
    self.assertGreater(gaussianSketch.get_estimate([2, 2]), 0.0)

    # We can also use a custom kernel when deserializing
    sk_bytes = sphericalSketch.serialize()
    sphericalRebuilt = density_sketch.deserialize(sk_bytes, UnitSphereKernel())
    self.assertEqual(sphericalSketch.get_estimate([1.001, 1]), sphericalRebuilt.get_estimate([1.001, 1]))

if __name__ == '__main__':
    unittest.main()
