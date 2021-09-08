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

from datasketches import CountSketch
import numpy as np


class CountSketchTest(unittest.TestCase):
    def test_count_sketch_example(self):
        nbuckets = 10
        nlevels = 5
        hh_threshold = 0.25
        sketch = CountSketch(num_buckets=nbuckets, num_levels=nlevels, phi=hh_threshold, rng_seed=1)
        self.assertTrue(sketch.is_empty())

        # we'll use a small number of distinct items so we
        # can use exponentially increasing weights and have
        # some frequent items, decreasing so we have some
        # small items inserted after a purge
        n = 8
        if sketch.max_num_items >= n:
            raise ValueError("Stream has fewer elements than are stored in the item heap. ")

        #  1. INSERTIONS --- Populate the sketch and iterate over the frequent items to get their counts
        for i in range(0, n):
            sketch.update(i, 2 ** (n - i))

        #  2. FREQUENT ITEMS --
        #  We can extract the frequent items which returns a list but here we cast it to a dict for later analysis.
        # There is no false positive or negative yet implemented.
        frequent_items = dict(sketch.get_frequent_items())
        print(f"Frequent items:\n", frequent_items)
        # For each item we return a point estimate of the frequency.
        for (item, count) in frequent_items.items():
            print(f'Item:{item}\tEstimate:{count}')

        #  3. POINT QUERIES -- We can also make point queries for items *not* in the frequent item list.
        for i in range(0, n):
            print(f'Item:{i}\tEstimate:{sketch.get_estimate(i)}')

        #  4.  MERGING --
        #  now create a second sketch with a lot of unique
        # values but all with equal weight (of 1) such that
        # the total weight is much larger than the first sketch
        sketch2 = CountSketch(num_buckets=nbuckets, num_levels=nlevels, phi=hh_threshold, rng_seed=2)
        wt = np.ceil(sketch.get_total_weight()).astype(np.int64)
        for i in range(0, 4*wt):
            sketch2.update(i)
        sketch.merge(sketch2)
        # we can see that the weight is much larger
        self.assertEqual(5 * wt, sketch.get_total_weight())  # The new total_weight should be the sum of both

        # This may leave the heap unchanged so let's adjust the stream by inserting a new item enough times to
        # enter the sketch after merging.
        wt = sketch.get_total_weight()
        light_item, light_count = sketch.get_frequent_items()[-1]
        num_to_insert = int(2*light_count)
        item_to_insert = sketch2.get_frequent_items()[-1][0]

        # nb. this doesn't mean the overall estimate will be ``num_to_insert``, but it should be close as it is a heavy
        # hitter for sketch3, which we merge into sketch 1
        print(f'Inserting {item_to_insert} {num_to_insert} times')
        sketch3 = CountSketch(num_buckets=nbuckets, num_levels=nlevels, phi=hh_threshold, rng_seed=3)
        new_stream = [item_to_insert] * num_to_insert
        for item in new_stream:
            sketch3.update(item)
        sketch.merge(sketch3)
        new_frequent_items = dict(sketch.get_frequent_items())
        print(new_frequent_items)
        self.assertTrue(item_to_insert in new_frequent_items.keys())
        self.assertEqual(sketch.get_estimate(item_to_insert), new_frequent_items[item_to_insert])

        # 5. WEIGHT is maintained under merging.
        total_weight = num_to_insert + wt
        self.assertEqual(total_weight, sketch.get_total_weight())
        self.assertFalse(sketch.merge(1))  # We can only merge two count_sketch objects.


if __name__ == '__main__':
    unittest.main()
