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

#ifndef DENSE_STORE_IMPL_HPP
#define DENSE_STORE_IMPL_HPP

#include <algorithm>
#include <bits/valarray_before.h>

#include "dense_store.hpp"

namespace datasketches {
template<typename Allocator>
DenseStore<Allocator>::DenseStore() :
  DenseStore(DEFAULT_ARRAY_LENGTH_GROWTH_INCREMENT)
{}

template<typename Allocator>
DenseStore<Allocator>::DenseStore(const int& array_length_growth_increment) :
  DenseStore(array_length_growth_increment, DEFAULT_ARRAY_LENGTH_OVERHEAD_RATIO)
{}

template<typename Allocator>
DenseStore<Allocator>::DenseStore(const int& array_length_growth_increment, const int& array_length_overhead) :
  Store<Allocator>(),
  total_count(0),
  min_index(std::numeric_limits<size_type>::max()),
  max_index(std::numeric_limits<size_type>::min()),
  array_length_growth_increment(array_length_growth_increment),
  array_length_overhead(array_length_growth_increment * array_length_overhead)
{}

template<typename Allocator>
void DenseStore<Allocator>::add(int index) {
  add(index, 1);
}

template<typename Allocator>
void DenseStore<Allocator>::add(int index, uint64_t count) {
  const size_type array_index = normalize(index);
  bins[array_index] += count;
  total_count += count;
}

template<typename Allocator>
void DenseStore<Allocator>::add(const Bin&  bin) {
  if (bin.getCount() == 0) {
    return;
  }
  add(bin.getIndex(), bin.getCount());
}

template<typename Allocator>
void DenseStore<Allocator>::clear() {
  bins.clear();
  total_count = 0;
  min_index = std::numeric_limits<size_type>::max();
  max_index = std::numeric_limits<size_type>::min();
}

template<typename Allocator>
bool DenseStore<Allocator>::is_empty() const {
  // return get_total_count() == 0;
  return max_index < min_index;
}

template<typename Allocator>
int DenseStore<Allocator>::get_max_index() const {
  return max_index;
}

template<typename Allocator>
int DenseStore<Allocator>::get_min_index() const {
  return min_index;
}

template<typename Allocator>
uint64_t DenseStore<Allocator>::get_total_count() const {
  return total_count;
}

template<typename Allocator>
void DenseStore<Allocator>::merge(const Store<Allocator>& other) {

}

template<typename Allocator>
typename DenseStore<Allocator>::size_type DenseStore<Allocator>::normalize(size_type index) {
  if (index < get_min_index() || index > get_max_index()) {
    extend_range(index, index);
  }
  return index - offset;
}

template<typename Allocator>
void DenseStore<Allocator>::extend_range(size_type new_min_index, size_type new_max_index) {
  new_min_index = std::min(new_min_index, get_min_index());
  new_max_index = std::max(new_max_index, get_max_index());

  if (is_empty()) {
    const size_type initial_length = get_new_length(new_min_index, new_max_index);
    if (bins.size() == 0 || initial_length >= bins.size()) {
      bins.resize(initial_length);
    }
    offset = new_min_index;
    min_index = new_min_index;
    max_index = new_max_index;
    adjust(new_min_index, new_max_index);
  } else if (new_min_index >= offset && new_max_index < (long) offset + bins.size()) {
    min_index = new_min_index;
    max_index = new_max_index;
  } else {
    // To avoid shifting too often when nearing the capacity of the array, we may grow it before
    // we actually reach the capacity.
    const int new_length = get_new_length(new_min_index, new_max_index);
    if (new_length > bins.size()) {
      bins.resize(new_length);
    }

    adjust(new_min_index, new_max_index);
  }
}

template<typename Allocator>
void DenseStore<Allocator>::shift_bins(size_type shift) {
  const size_type min_arr_index = min_index - offset;
  const size_type max_arr_index = max_index - offset;

  if (shift > 0) {
    std::move(bins.begin() + min_arr_index, bins.begin() + max_arr_index + 1, bins.begin() + min_arr_index + shift);
    std::fill(bins.begin() + min_arr_index, bins.begin() + shift + 1, 0);
  } else {
    std::move_backward(bins.begin() + min_arr_index, bins.begin() + max_arr_index + 1, bins.begin() + min_arr_index + shift);
    std::fill(bins.begin() + max_arr_index + 1 + shift, bins.begin() + max_arr_index + 1, 0);
  }

  offset -= shift;
}

template<typename Allocator>
void DenseStore<Allocator>::center_bins(size_type new_min_index, size_type new_max_index) {
  const size_type middle_index = new_min_index + (new_max_index - new_min_index) / 2;
  shift_bins(offset + bins.length() / 2 - middle_index);

  min_index = new_min_index;
  max_index = new_max_index;
}

template<typename Allocator>
typename DenseStore<Allocator>::size_type DenseStore<Allocator>::get_new_length(size_type new_min_index, size_type new_max_index) const {
  const size_type desired_length = new_max_index - new_min_index + 1;
  return ((desired_length + array_length_overhead - 1) / array_length_growth_increment + 1) * array_length_growth_increment;
}

}

#endif //DENSE_STORE_IMPL_HPP
