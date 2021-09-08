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


class StreamingHeap:
    def __init__(self, max_heap_size):
        """
        This is a basic implementation of a heap used for maintaining heavy hitters
        with the CountSketch.

        We keep a dictionary of items in a (key, value) pair as (item, estimate_count)
        in the dictionary.

        Note that this *** IS NOT *** strictly a heap in the current implementation.
        At least one optimisation would be to store the items in a heap data structure.
        This would avoid the need for sorting in the third line of _set_heap_minimum.
        """
        self.max_heap_size = max_heap_size
        self.heap = {}  # {-np.inf: -np.inf}
        self.minimum_count = - np.inf  # Set baseline for comparisons that will *always* be less than any inserted value

    def _set_heap_minimum(self):
        """
        Sets the minimum value of the heap for quicker threshold checking over the stream.
        """
        k = np.array(list(self.heap.keys()))
        v = np.array(list(self.heap.values()))
        v_min_id = np.argmin(v)  # !!! Arbitrary tie breaking for the first item !!!
        self.minimum_count = v[v_min_id]
        self.minimum_count_id = k[v_min_id]

    def _is_space_in_heap(self):
        """
        Return True if the heap is not full (i.e. has fewer keys than ``self.max_heap_size'') and False otherwise
        """
        if len(self.heap) < self.max_heap_size:
            return True
        return False

    def _item_enter_sketch(self, estimate_count):
        """
        Checks if item with value estimate_count is larger than the current minimum.
        If YES then return True so that we can permit the item into the sketch.
        """
        if estimate_count > self.minimum_count:
            return True
        return False

    def push(self, item, estimate_count):
        """
        Inserts key ``item'' into the heap with value ``estimate_count''
        """
        if item in self.heap.keys() or self._is_space_in_heap():
            # If the item is in the current heap then simply adjust its counter
            # Alternatively, if there is room in the heap, then just add a new counter.
            self.heap[item] = estimate_count
        elif not self._item_enter_sketch(estimate_count):
            # Does nothing and returns if the item does not have large enough count to enter the sketch.
            #  print(f'Item {item}: count {estimate_count} too small')
            return
        else:
            # The heap is full and the item is not in the heap so we remove (key, value) pair at
            # heap root [i.e. the node root = (min_key, min_count) ] and
            # insert a new (key, value) = (item, estimate_count) to the heap in place of (min_key, min_count)
            # Then reset the minimum count and value id for the heap.
            self.heap.pop(self.minimum_count_id)
            self.heap[item] = estimate_count
        self._set_heap_minimum()

    def merge(self, other_streaming_heap):
        """
        The merge step on heaps is done naively and could be improved by using min_heaps rather than just dicts.
        :param other_streaming_heap:
        :return:
        """
        try:
            all_k_v = self.heap.copy()
            all_k_v.update(other_streaming_heap.heap)
            sorted_values = sorted(all_k_v,  key=all_k_v.get, reverse=True)
            # Now we will delete all keys in self.heap and repopulate until it is large enough.
            self.heap = {}
            for i in range(self.max_heap_size):
                k = sorted_values[i]
                self.heap[k] = all_k_v[k]
            all_k_v.clear()  # Deletes the dictionary.
            self._set_heap_minimum()
        except TypeError:
            raise TypeError("Argument must be a StreamingHeap object")

    def keys(self):
        """
        Overload the keys() functionality from dictionary
        """
        return self.heap.keys()

    def __str__(self):
        """
        Returns the string version of the dictionary to print
        Overloads the print() function on a dict
        """
        return '{}'.format(self.heap)
