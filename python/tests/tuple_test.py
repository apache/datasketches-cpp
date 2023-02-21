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

from datasketches import update_tuple_sketch
from datasketches import compact_tuple_sketch, tuple_union
from datasketches import tuple_intersection, tuple_a_not_b
from datasketches import tuple_jaccard_similarity
from datasketches import tuple_jaccard_similarity, PyIntsSerDe
from datasketches import AccumulatorPolicy, MaxIntPolicy, MinIntPolicy
from datasketches import update_theta_sketch

class TupleTest(unittest.TestCase):
    def test_tuple_basic_example(self):
        lgk = 12    # 2^k = 4096 rows in the table
        n = 1 << 18 # ~256k unique values

        # create a sketch and inject some values -- summary is 2 so we can sum them
        # and know the reuslt
        sk = self.generate_tuple_sketch(AccumulatorPolicy(), n, lgk, value=2)

        # we can check that the upper and lower bounds bracket the
        # estimate, without needing to know the exact value.
        self.assertLessEqual(sk.get_lower_bound(1), sk.get_estimate())
        self.assertGreaterEqual(sk.get_upper_bound(1), sk.get_estimate())

        # because this sketch is deterministically generated, we can
        # also compare against the exact value
        self.assertLessEqual(sk.get_lower_bound(1), n)
        self.assertGreaterEqual(sk.get_upper_bound(1), n)

        # compact and serialize for storage, then reconstruct
        sk_bytes = sk.compact().serialize(PyIntsSerDe())
        new_sk = compact_tuple_sketch.deserialize(sk_bytes, serde=PyIntsSerDe())

        # estimate remains unchanged
        self.assertFalse(sk.is_empty())
        self.assertEqual(sk.get_estimate(), new_sk.get_estimate())

        # we can also iterate over the sketch entries
        # the iterator provides a (hashkey, summary) pair where the
        # first value is the raw hash value and the second the summary
        count = 0
        cumSum = 0
        for pair in new_sk:
          self.assertLess(pair[0], new_sk.get_theta64())
          count += 1
          cumSum += pair[1]
        self.assertEqual(count, new_sk.get_num_retained())
        self.assertEqual(cumSum, 2 * new_sk.get_num_retained())

        # we can even create a tuple sketch from an existing theta sketch
        # as long as we provide a summary to use
        theta_sk = update_theta_sketch(lgk)
        for i in range(n, 2*n):
          theta_sk.update(i)
        cts = compact_tuple_sketch(theta_sk, 5)
        cumSum = 0
        for pair in cts:
          cumSum += pair[1]
        self.assertEqual(cumSum, 5 * cts.get_num_retained())


    def test_tuple_set_operations(self):
        lgk = 12    # 2^k = 4096 rows in the table
        n = 1 << 18 # ~256k unique values

        # we'll have 1/4 of the values overlap
        offset = int(3 * n / 4) # it's a float w/o cast

        # create a couple sketches and inject some values, with different summaries
        sk1 = self.generate_tuple_sketch(AccumulatorPolicy(), n, lgk, value=5)
        sk2 = self.generate_tuple_sketch(AccumulatorPolicy(), n, lgk, value=7, offset=offset)

        # UNIONS
        # create a union object
        union = tuple_union(MaxIntPolicy(), lgk)
        union.update(sk1)
        union.update(sk2)

        # getting result from union returns a compact_theta_sketch
        # compact theta sketches can be used in additional unions
        # or set operations but cannot accept further item updates
        result = union.get_result()
        self.assertTrue(isinstance(result, compact_tuple_sketch))

        # since our process here is deterministic, we have
        # checked and know the exact answer is within one
        # standard deviation of the estimate
        self.assertLessEqual(result.get_lower_bound(1), 7 * n / 4)
        self.assertGreaterEqual(result.get_upper_bound(1), 7 * n / 4)

        # we unioned two equal-sized sketches with overlap and used
        # the max value as the resulting summary, meaning we should
        # have more summaries with value 7 than value 5 in the result
        count5 = 0
        count7 = 0
        for pair in result:
          if pair[1] == 5:
            count5 += 1
          elif pair[1] == 7:
            count7 += 1
          else:
            self.fail()
        self.assertLess(count5, count7)

        # INTERSECTIONS
        # create an intersection object
        intersect = tuple_intersection(MinIntPolicy()) # no lg_k
        intersect.update(sk1)
        intersect.update(sk2)

        # has_result() indicates the intersection has been used,
        # although the result may be the empty set
        self.assertTrue(intersect.has_result())

        # as with unions, the result is a compact sketch
        result = intersect.get_result()
        self.assertTrue(isinstance(result, compact_tuple_sketch))

        # we know the sets overlap by 1/4
        self.assertLessEqual(result.get_lower_bound(1), n / 4)
        self.assertGreaterEqual(result.get_upper_bound(1), n / 4)

        # in this example, we intersected the sketches and took the
        # min value as the resulting summary, so all summaries
        # must be exactly equal to that value
        count5 = 0
        for pair in result:
          if pair[1] == 5:
            count5 += 1
          else:
            self.fail()
        self.assertEqual(count5, result.get_num_retained())

        # A NOT B
        # create an a_not_b object
        anb = tuple_a_not_b() # no lg_k or policy
        result = anb.compute(sk1, sk2)

        # as with unions, the result is a compact sketch
        self.assertTrue(isinstance(result, compact_tuple_sketch))

        # we know the sets overlap by 1/4, so the remainder is 3/4
        self.assertLessEqual(result.get_lower_bound(1), 3 * n / 4)
        self.assertGreaterEqual(result.get_upper_bound(1), 3 * n / 4)

        # here, we have only values with a summary of 5 as any keys that
        # existed in both sketches were removed
        count5 = 0
        for pair in result:
          if pair[1] == 5:
            count5 += 1
          else:
            self.fail()
        self.assertEqual(count5, result.get_num_retained())

        # JACCARD SIMILARITY
        # Jaccard Similarity measure returns (lower_bound, estimate, upper_bound)
        # and does not examine summaries, even for (dis)similarity tests.
        jac = tuple_jaccard_similarity.jaccard(sk1, sk2)

        # we can check that results are in the expected order
        self.assertLess(jac[0], jac[1])
        self.assertLess(jac[1], jac[2])

        # checks for sketch equivalence
        self.assertTrue(tuple_jaccard_similarity.exactly_equal(sk1, sk1))
        self.assertFalse(tuple_jaccard_similarity.exactly_equal(sk1, sk2))

        # we can apply a check for similarity or dissimilarity at a
        # given threshold, at 97.7% confidence.

        # check that the Jaccard Index is at most (upper bound) 0.2.
        # exact result would be 1/7
        self.assertTrue(tuple_jaccard_similarity.dissimilarity_test(sk1, sk2, 0.2))

        # check that the Jaccard Index is at least (lower bound) 0.7
        # exact result would be 3/4, using result from A NOT B test
        self.assertTrue(tuple_jaccard_similarity.similarity_test(sk1, result, 0.7))


    # Generates a basic tuple sketch with a fixed value for each update
    def generate_tuple_sketch(self, policy, n, lgk, value, offset=0):
      sk = update_tuple_sketch(policy, lgk)
      for i in range(0, n):
        sk.update(i + offset, value)
      return sk

if __name__ == '__main__':
    unittest.main()
