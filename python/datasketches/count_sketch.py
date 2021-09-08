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

import numpy as np
from random import randint, seed
from .streaming_heap import StreamingHeap


class CountSketch:
    def __init__(self,  num_buckets: int, num_levels: int, phi, rng_seed=100):
        """
        # TODO: add string update functionality.
        :param num_buckets:int -- number of columns in the sketch table
        :param num_levels:int -- number of rows in the sketch table
        :param phi:float -- The threshold for heavy hitters
        :param rng_seed:int -- seed for the randomisation

        Attributes:
        - self.p  :: int - a large prime used to generate the random hashes.
        - self.w :: int - the number of hash buckets for the table
        - self.d :: int - the number of levels that are repeated
        - self.table :: np.ndarray of type float that is the table that is updated on viewing data.


        HELPFUL SOURCES:
        http://web.stanford.edu/class/archive/cs/cs166/cs166.1166/lectures/11/Small11.pdf
        http://web.stanford.edu/class/archive/cs/cs166/cs166.1146/lectures/12/Small12.pdf
        https://people.cs.umass.edu/~mcgregor/711S12/sketches1.pdf
        https://cs.au.dk/~gerth/advising/thesis/jonas-nicolai-hovmand_morten-houmoeller-nygaard.pdf

        Denote F2 = ||f||_2 where f is the frequency vector underlying the data stream.
        eg. if stream = (1, 1, 2, 1, 5) then f = (0, 3, 1, 0, 0, 1).
        If the stream is weighted i.e. we receive (item, weight) pairs, then the same idea applies:
        stream = [(1, 4), (2, 1) (1, -1), (5, 1)] also has f = (0, 3, 1, 0, 0, 1) -- nb using 0-based indexing for
        consistency with python.

        The a-heavy hitters problem is to identify all items i for which f[i] > a*F2
        Solving this problem requires linear space in the worst-case so a relaxed version is solved: to return
        b-heavy hitters for b = a - t and t > 0.
        Hence, we permit some _false positives_ : items that are flagged as being b-heavy, but may not be a-heavy.

        A CountSketch can achieve this aim in small space by maintaining a sketch that is used to estimate f[i].
        Specifically, the CountSketch returns an estimate g[i] for which
        f[i] - epsilon*F2 <= g[i] <= f[i] + epsilon*F2.
        The value epsilon is between 0 and 1 and is the worst-case error for estimating the frequency f[i] using the
        value g[i].
        epsilon can be explicitly calculated using parameters of the sketch self.w and self.d as seen in the function
        def get_epsilon(self).

        We return a set i of heavy indices by using the sketch and finding the (relaxed) b-heavy hitters.
        The CountSketch guarantee ensures f[i] - epsilon*F2 <= g[i] so we will opt to find all i such that:
        g[i] >= (b - epsilon)*F2 or equivalently g[i] >= ( (a - t) - epsilon)*F2.

        We *set* phi ( = b - t ) and do not use the (a - t) setup and epsilon is explicitly known.
        Note that there are two sources of approximation:
         - the ``t'' is the heavy hitter approximate relaxation parameter
         - the ``epsilon'' is the frequency estimation parameter.
        """
        seed(rng_seed)
        self.p = 2 ** 31 - 1
        self.w = num_buckets
        self.d = num_levels
        self.table = np.zeros((self.d, self.w), dtype=float)
        self._init_hashes()
        self.phi = phi
        assert(self.phi <= 1.0), f"Phi={self.phi:.5f} but cannot be larger than 1."
        self.total_weight = 0.0  # Sum of all weights seen and is aka the L1 norm of the underlying frequency vector.
        self.max_num_items = np.ceil(1./self.phi).astype(np.int64)
        self.heavy_hitter_detector = StreamingHeap(self.max_num_items)
        self.merged_sketches = []  # A list of all sketches that have been merged with self.

    def _init_hashes(self):
        """
        Initialises the hash functions for bucket and sign selection by generating (a,b) pairs for the hash family.
        A new (a,b) pair is necessary for each of the hash functions
        """
        self.bucket_hash_params = {i: self._get_ab_hash() for i in range(self.d)}
        self.sign_hash_params = {i: self._get_ab_hash() for i in range(self.d)}

    def _get_ab_hash(self):
        """
        # TODO - check the nonzero property on a is correct
        We need 0 <= a <= self.p - 1 and 0 <= b <= self.p - 1 as discussed on page 18
        of https://people.cs.umass.edu/~mcgregor/711S12/sketches1.pdf
        :return: the (a,b) pair used to define the hash family
        """
        return randint(0, self.p - 1), randint(0, self.p - 1)  # random (a,b) pairs from ([p], [p])

    def _bucket_hash(self, x: int, a: int, b: int, buckets: int):
        """
        Generic function for generating 2-wise independent hash functions.
        We use this function for the bucket locations with buckets <- self.num_buckets.
        It is also used to generate the random signs with buckets <- 2 -- see `` def _sign_hash ''

        :param x: stream item
        :param a:
        :param b:
        :param buckets:
        :return: h:int the hash value (aka bucket index) for item x observed in the stream.
        """
        h = (a * x + b) % self.p
        h = h % buckets
        return h

    def _sign_hash(self, x: int, a: int, b: int):
        """
        Returns the 2-wise independent sign hash.
        This generates the signs when items are put into buckets.
        :param x:stream item
        :param a:
        :param b:
        :return:s -- int the sign +1 or -1 used for the hashing.
        """
        s = 2.*self._bucket_hash(x, a, b, 2) - 1.
        return s

    def _insert(self, item: int, weight=1.0):
        """
        Inserts the item into the sketch table
        :param item:
        :param weight:
        """
        if not (isinstance(item, np.integer) or isinstance(item, int)):
            # this checks for ``int`` and ``np.int*` for any input item
            raise TypeError("Input item must be an int.")
        for ii in range(self.d):
            a_bucket, b_bucket = self.bucket_hash_params[ii]  # Gets the (a,b) pair used for *bucket* hashes at level ii
            a_sign, b_sign = self.sign_hash_params[ii]  # Gets the (a,b) pair used for *sign* hashes at level ii
            bucket = self._bucket_hash(item, a_bucket, b_bucket, self.w)
            sign = self._sign_hash(item, a_sign, b_sign)
            self.table[ii, bucket] += sign*weight

    def update(self, item: int, weight=1.0):
        """
        Updates the sketch by:
         1. Inserting (item, weight) into self.table
         2. Estimating the current frequency and pushing it into the heavy_hitter_detector
         (nb. item only pushed into the detector if it has large enough frequency.
         This logic is dealt with in the detector class itself.)
        :param item:int
        :param weight:float
        """
        self.total_weight += weight
        self._insert(item, weight)
        self.heavy_hitter_detector.push(item, self.get_estimate(item))

    def get_epsilon(self):
        """
        :return: an estimate of the worst-case epsilon error achieved by the sketch for frequency estimation.
        """
        return np.sqrt(np.e / self.w)

    def get_estimate(self, item: int):
        """
        :param item:int -- the identifier of the item whose frequency is being queried.
        :return:np.median(_estimates) -- The count sketch frequency estimate.
        """
        _estimates = self._get_frequency_estimate(item)
        for sk in self.merged_sketches:
            _estimates += sk._get_frequency_estimate(item)
        return np.median(_estimates)  # TODO - replace this with np.max(0.0, np.median(_estimate))?

    def _get_frequency_estimate(self, item: int):
        """
        :param item:
        :return: _estimates -- an array of the frequency estimate where each entry corresponds to a level of the sketch.
        """
        _buckets = {_: self._bucket_hash(item, self.bucket_hash_params[_][0], self.bucket_hash_params[_][1], self.w)
                    for _ in range(self.d)}
        _signs = {_: self._sign_hash(item, self.sign_hash_params[_][0], self.sign_hash_params[_][1])
                  for _ in range(self.d)}
        _estimates = np.array([self.table[_, _buckets[_]] * _signs[_] for _ in range(self.d)])
        return _estimates

    def get_frequent_items(self):
        """
        :return: a list of the heavy hitters and their frequencies.
        """
        for k in self.heavy_hitter_detector.heap.keys():
            self.heavy_hitter_detector.heap[k] = self.get_estimate(k)
        return list(self.heavy_hitter_detector.heap.items())

    def get_total_weight(self):
        """
        Returns the total weight inserted on the stream.
        Is equivalent to the mass inserted over the stream, or the ell_1 norm of the
        underlying frequency vector.
        """
        return self.total_weight

    def is_empty(self):
        """
        Returns True if the sketch self.table is empty and is False otherwise.
        """
        if not np.any(self.table):
            #  All zeros array
            return True
        else:
            # There is a nonzero in self.table
            return False

    def merge(self, other_count_sketch):
        """
        :param other_count_sketch: another count_sketch object to be merged into self.

        The count sketch is a linear sketch so we can simple add the data structures self.table
        to other_count_sketch.table.
        Then we need to adjust the dictionaries of heavy hitters.
        """
        try:
            self.merged_sketches.append(other_count_sketch)
            self.table += other_count_sketch.table
            self.total_weight += other_count_sketch.total_weight
            # Call a naive merge on StreamingHeap (could be improved by using min_heaps rather than just dicts.)
            self.heavy_hitter_detector.merge(other_count_sketch.heavy_hitter_detector)
        except AttributeError:
            AttributeError("Argument must be a count sketch object")

    def __str__(self):
        return (
            '### Count sketch summary:\n'
            f' num. buckets           : {self.w}\n'
            f' num. levels            : {self.d}\n'
            f' worst-case error       : {(self.get_epsilon()):.5f}\n'
            f' Heavy hitter threshold : {self.phi:.5f}\n'
            '### End sketch summary')
