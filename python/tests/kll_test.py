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
from datasketches import (kll_ints_sketch, kll_floats_sketch, 
                          vector_of_kll_ints_sketches,
                          vector_of_kll_floats_sketches)
import numpy as np


class KllTest(unittest.TestCase):
    def test_kll_example(self):
      k = 160
      n = 2 ** 20

      # create a sketch and inject ~1 million N(0,1) points
      kll = kll_floats_sketch(k)
      for i in range(0, n):
        kll.update(np.random.randn())

      # 0 should be near the median
      self.assertAlmostEqual(0.5, kll.get_rank(0.0), delta=0.025)
      
      # the median should be near 0
      self.assertAlmostEqual(0.0, kll.get_quantile(0.5), delta=0.025)

      # we also track the min/max independently from the rest of the data
      # which lets us know the full observed data range
      self.assertLessEqual(kll.get_min_value(), kll.get_quantile(0.01))
      self.assertLessEqual(0.0, kll.get_rank(kll.get_min_value()))
      self.assertGreaterEqual(kll.get_max_value(), kll.get_quantile(0.99))
      self.assertGreaterEqual(1.0, kll.get_rank(kll.get_max_value()))

      # we can also extract a list of values at a time,
      # here the values should give us something close to [-2, -1, 0, 1, 2].
      # then get the CDF, which will return something close to
      # the original values used in get_quantiles()
      # finally, can check the normalized rank error bound
      pts = kll.get_quantiles([0.0228, 0.1587, 0.5, 0.8413, 0.9772])
      cdf = kll.get_cdf(pts)  # include 1.0 at end to account for all probability mass
      self.assertEqual(len(cdf), len(pts)+1)
      err = kll.normalized_rank_error(False)
      self.assertEqual(err, kll_floats_sketch.get_normalized_rank_error(k, False))

      # and a few basic queries about the sketch
      self.assertFalse(kll.is_empty())
      self.assertTrue(kll.is_estimation_mode())
      self.assertEqual(kll.get_n(), n)
      self.assertLess(kll.get_num_retained(), n)

      # merging itself will double the number of items the sketch has seen
      kll.merge(kll)
      self.assertEqual(kll.get_n(), 2*n)

      # we can then serialize and reconstruct the sketch
      kll_bytes = kll.serialize()
      new_kll = kll.deserialize(kll_bytes)
      self.assertEqual(kll.get_num_retained(), new_kll.get_num_retained())
      self.assertEqual(kll.get_min_value(), new_kll.get_min_value())
      self.assertEqual(kll.get_max_value(), new_kll.get_max_value())
      self.assertEqual(kll.get_quantile(0.7), new_kll.get_quantile(0.7))
      self.assertEqual(kll.get_rank(0.0), new_kll.get_rank(0.0))


    def test_kll_ints_sketch(self):
        k = 100
        n = 10
        kll = kll_ints_sketch(k)
        for i in range(0, n):
          kll.update(i)

        self.assertEqual(kll.get_min_value(), 0)
        self.assertEqual(kll.get_max_value(), n-1)
        self.assertEqual(kll.get_n(), n)
        self.assertFalse(kll.is_empty())
        self.assertFalse(kll.is_estimation_mode()) # n < k

        pmf = kll.get_pmf([round(n/2)])
        self.assertIsNotNone(pmf)
        self.assertEqual(len(pmf), 2)

        cdf = kll.get_cdf([round(n/2)])
        self.assertIsNotNone(cdf)
        self.assertEqual(len(cdf), 2)

        self.assertEqual(kll.get_quantile(0.5), round(n/2))
        quants = kll.get_quantiles([0.25, 0.5, 0.75])
        self.assertIsNotNone(quants)
        self.assertEqual(len(quants), 3)

        self.assertEqual(kll.get_rank(round(n/2)), 0.5)

        # merge self
        kll.merge(kll)
        self.assertEqual(kll.get_n(), 2 * n)

        sk_bytes = kll.serialize()
        self.assertTrue(isinstance(kll_ints_sketch.deserialize(sk_bytes), kll_ints_sketch))

    def test_kll_floats_sketch(self):
      # already tested ints and it's templatized, so just make sure it instantiates properly
      k = 75
      kll = kll_floats_sketch(k)
      self.assertTrue(kll.is_empty())


class KllSketchesTest(unittest.TestCase):
    def test_kll_sketches_example(self):
      k = 200
      d = 3
      n = 2 ** 20

      # create a sketch and inject ~1 million N(0,1) points
      kll = vector_of_kll_floats_sketches(k, d)
      # Track the min/max for each sketch to test later
      smin = np.zeros(d) + np.inf
      smax = np.zeros(d) - np.inf

      for i in range(0, n):
        dat  = np.random.randn(d)
        smin = np.amin([smin, dat], axis=0)
        smax = np.amax([smax, dat], axis=0)
        kll.update(dat)

      # 0 should be near the median
      np.testing.assert_allclose(0.5, kll.get_ranks(0.0), atol=0.025)
      # the median should be near 0
      np.testing.assert_allclose(0.0, kll.get_quantiles(0.5), atol=0.025)
      # we also track the min/max independently from the rest of the data
      # which lets us know the full observed data range
      np.testing.assert_allclose(kll.get_min_values(), smin)
      np.testing.assert_allclose(kll.get_max_values(), smax)
      np.testing.assert_array_less(kll.get_min_values(), kll.get_quantiles(0.01)[:,0])
      np.testing.assert_array_less(kll.get_quantiles(0.99)[:,0], kll.get_max_values())

      # we can also extract a list of values at a time,
      # here the values should give us something close to [-2, -1, 0, 1, 2].
      # then get the CDF, which will return something close to
      # the original values used in get_quantiles()
      # finally, can check the normalized rank error bound
      pts = kll.get_quantiles([0.0228, 0.1587, 0.5, 0.8413, 0.9772])
      # use the mean pts for the CDF, include 1.0 at end to account for all probability mass
      meanpts = np.mean(pts, axis=0)
      cdf = kll.get_cdf(meanpts)
      self.assertEqual(cdf.shape[0], pts.shape[0])
      self.assertEqual(cdf.shape[1], pts.shape[1]+1)

      # and a few basic queries about the sketch
      self.assertFalse(np.all(kll.is_empty()))
      self.assertTrue(np.all(kll.is_estimation_mode()))
      self.assertTrue(np.all(kll.get_n() == n))
      self.assertTrue(np.all(kll.get_num_retained() < n))

      # we can combine sketches across all dimensions and get the reuslt
      result = kll.collapse()
      self.assertEqual(result.get_n(), d * n)

      # merging a copy of itself will double the number of items the sketch has seen
      kll_copy = vector_of_kll_floats_sketches(kll)
      kll.merge(kll_copy)
      np.testing.assert_equal(kll.get_n(), 2*n)

      # we can then serialize and reconstruct the sketch
      kll_bytes = kll.serialize() # serializes each sketch as a list
      new_kll = vector_of_kll_floats_sketches(k, d)
      for s in range(len(kll_bytes)):
        new_kll.deserialize(kll_bytes[s], s)

      # everything should be exactly equal
      np.testing.assert_equal(kll.get_num_retained(), new_kll.get_num_retained())
      np.testing.assert_equal;(kll.get_min_values(), new_kll.get_min_values())
      np.testing.assert_equal(kll.get_max_values(), new_kll.get_max_values())
      np.testing.assert_equal(kll.get_quantiles(0.7), new_kll.get_quantiles(0.7))
      np.testing.assert_equal(kll.get_ranks(0.0), new_kll.get_ranks(0.0))

    def test_kll_ints_sketches(self):
      # already tested floats and it's templatized, so just make sure it instantiates properly
      k = 100
      d = 5
      kll = vector_of_kll_ints_sketches(k, d)
      self.assertTrue(np.all(kll.is_empty()))


if __name__ == '__main__':
    unittest.main()
