/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef COLLAPSING_LOWEST_DENSE_STORE_IMPL_HPP
#define COLLAPSING_LOWEST_DENSE_STORE_IMPL_HPP

#include "collapsing_lowest_dense_store.hpp"

namespace datasketches {
template<typename Allocator>
CollapsingLowestDenseStore<Allocator>::CollapsingLowestDenseStore(size_type max_num_bins): CollapsingDenseStore<CollapsingLowestDenseStore<Allocator>, Allocator>(max_num_bins){}

template<typename Allocator>
CollapsingLowestDenseStore<Allocator>* CollapsingLowestDenseStore<Allocator>::copy() const {
  using StoreAlloc = typename std::allocator_traits<Allocator>::template rebind_alloc<CollapsingLowestDenseStore<Allocator>>;
  StoreAlloc alloc(this->bins.get_allocator());
  return new (alloc.allocate(1)) CollapsingLowestDenseStore<Allocator>(*this);
}

template<typename Allocator>
template<class OtherDerived>
void CollapsingLowestDenseStore<Allocator>::merge(const DenseStore<OtherDerived, Allocator>& other) {
    for (auto it = other.rbegin(); it != other.rend(); ++it) {
      this->add(*it);
    }
}

template<typename Allocator>
void CollapsingLowestDenseStore<Allocator>::merge(const CollapsingLowestDenseStore<Allocator>& other) {
  if (other.is_empty()) {
    return;
  }

  if (other.min_index < this->min_index || other.max_index > this->max_index) {
    this->extend_range(other.min_index, other.max_index);
  }

  size_type index = other.min_index;
  for (; index < this->min_index && index <= other.max_index; index++) {
    this->bins[0] += other.bins[index - other.offset];
  }
  for (; index < other.max_index; index++) {
    this->bins[index - this->offset] += other.bins[index - other.offset];
  }
  // This is a separate test so that the comparison in the previous loop is strict (>) and handles
  // other.min_index = Integer.MIN_VALUE.
  if (index == other.max_index) {
    this->bins[index - this->offset] += other.bins[index - other.offset];
  }
}

template <typename Allocator>
typename CollapsingLowestDenseStore<Allocator>::size_type CollapsingLowestDenseStore<Allocator>::normalize(size_type index) {
  if (index < this->min_index) {
    if (this->is_collapsed) {
      return static_cast<size_type>(0);
    }
    this->extend_range(index);
    if (this->is_collapsed) {
      return static_cast<size_type>(0);
    }
  } else if (index > this->max_index) {
    this->extend_range(index);
  }

  return index - this->offset;
}

template <typename Allocator>
void CollapsingLowestDenseStore<Allocator>::adjust(size_type new_min_index, size_type new_max_index) {
  if (new_max_index - new_min_index + 1 > this->bins.size()) {
    // The range of indices is too wide, buckets of lowest indices need to be collapsed.
    new_min_index = new_max_index - this->bins.size() + 1;

    if (new_min_index >= this->max_index) {
      // There will be only one non-empty bucket.
      const double total_count = this->get_total_count();
      this->reset_bins();
      this->offset = new_min_index;
      this->min_index = new_min_index;
      this->bins[0] = total_count;
    } else {
      const size_type shift = this->offset - new_min_index;
      if (shift < 0) {
        // Collapse the buckets.
        const double collapsed_count = this->get_total_count(this->min_index, new_min_index - 1);
        this->reset_bins(this->min_index, new_min_index - 1);
        this->bins[new_min_index - this->offset] += collapsed_count;
        this->min_index = new_min_index;
        // Shift the buckets to make room for new_min_index.
        this->shift_bins(shift);
      } else {
        // Shift the buckets to make room for new_max_index.
        this->shift_bins(shift);
        this->min_index = new_min_index;
      }
    }
    this->max_index = new_max_index;
    this->is_collapsed = true;
  } else {
    this->center_bins(new_min_index, new_max_index);
  }
}

}

#endif //COLLAPSING_LOWEST_DENSE_STORE_IMPL_HPP
