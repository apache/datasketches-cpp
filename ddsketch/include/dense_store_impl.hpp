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
#include "dense_store.hpp"

namespace datasketches {
template<typename Allocator>
constexpr int DenseStore<Allocator>::DEFAULT_ARRAY_LENGTH_GROWTH_INCREMENT;

template<typename Allocator>
constexpr double DenseStore<Allocator>::DEFAULT_ARRAY_LENGTH_OVERHEAD_RATIO;

template<typename Allocator>
DenseStore<Allocator>::DenseStore() :
  DenseStore(DEFAULT_ARRAY_LENGTH_GROWTH_INCREMENT)
{}

template<typename Allocator>
DenseStore<Allocator>::DenseStore(const int& array_length_growth_increment) :
  DenseStore(array_length_growth_increment, array_length_growth_increment * DEFAULT_ARRAY_LENGTH_OVERHEAD_RATIO)
{}

template<typename Allocator>
DenseStore<Allocator>::DenseStore(const int& array_length_growth_increment, const int& array_length_overhead):
  offset(0),
  min_index(std::numeric_limits<size_type>::max()),
  max_index(std::numeric_limits<size_type>::min()),
  array_length_growth_increment(array_length_growth_increment),
  array_length_overhead(array_length_overhead)
{}

template<typename Allocator>
void DenseStore<Allocator>::add(int index) {
  add(index, 1);
}

template<typename Allocator>
void DenseStore<Allocator>::add(int index, uint64_t count) {
  if (count == 0) {
    return;
  }
  const size_type array_index = normalize(index);
  bins[array_index] += count;
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
  bins.resize(bins.size(), 0);
  min_index = std::numeric_limits<size_type>::max();
  max_index = std::numeric_limits<size_type>::min();
  offset = 0;
}

template<typename Allocator>
bool DenseStore<Allocator>::is_empty() const {
  // return get_total_count() == 0;
  return max_index < min_index;
}

template<typename Allocator>
typename DenseStore<Allocator>::size_type DenseStore<Allocator>::get_max_index() const {
  if (is_empty()) {
    throw std::runtime_error("store is empty");
  }
  return max_index;
}

template<typename Allocator>
typename DenseStore<Allocator>::size_type DenseStore<Allocator>::get_min_index() const {
  if (is_empty()) {
    throw std::runtime_error("store is empty");
  }
  return min_index;
}

template<typename Allocator>
uint64_t DenseStore<Allocator>::get_total_count() const {
  return get_total_count(min_index, max_index);
}

template<typename Allocator>
uint64_t DenseStore<Allocator>::get_total_count(size_type from_index, size_type to_index) const {
  if (is_empty()) {
    return 0;
  }

  uint64_t total_count = 0;
  size_type from_array_index = std::max(from_index - offset, static_cast<size_type>(0));
  size_type to_array_index = std::min(to_index - offset, static_cast<size_type>(bins.size() - 1));
  for (size_type index = from_array_index; index <= to_array_index; index++) {
    total_count += bins[index];
  }

  return total_count;
}

template<typename Allocator>
typename DenseStore<Allocator>::iterator DenseStore<Allocator>::begin() const {
  if (is_empty()) {
    return end();
  }
  return DenseStore<Allocator>::iterator(this->bins, this->min_index, this->max_index, this->offset);
}

template<typename Allocator>
typename DenseStore<Allocator>::iterator DenseStore<Allocator>::end() const {
  return DenseStore<Allocator>::iterator(this->bins, this->max_index + 1, this->max_index, this->offset);
}

template<typename Allocator>
typename DenseStore<Allocator>::reverse_iterator DenseStore<Allocator>::rbegin() const {
  if (is_empty()) {
    return rend();
  }
  return DenseStore<Allocator>::reverse_iterator(this->bins, this->max_index, this->min_index, this->offset);
}

template<typename Allocator>
typename DenseStore<Allocator>::reverse_iterator DenseStore<Allocator>::rend() const {
  return DenseStore<Allocator>::reverse_iterator(this->bins, this->min_index - 1, this->min_index, this->offset);
}


template<typename Allocator>
typename DenseStore<Allocator>::size_type DenseStore<Allocator>::normalize(size_type index) {
  if (index < get_min_index() || index > get_max_index()) {
    extend_range(index, index);
  }
  return index - offset;
}

template<typename Allocator>
void DenseStore<Allocator>::extend_range(size_type index) {
  extend_range(index, index);
}

template<typename Allocator>
void DenseStore<Allocator>::extend_range(size_type new_min_index, size_type new_max_index) {
  new_min_index = std::min(new_min_index, min_index);
  new_max_index = std::max(new_max_index, max_index);

  if (is_empty()) {
    const size_type initial_length = get_new_length(new_min_index, new_max_index);
    if (bins.empty() || initial_length >= static_cast<size_type>(bins.size())) {
      bins.resize(initial_length);
    }
    offset = new_min_index;
    min_index = new_min_index;
    max_index = new_max_index;
    adjust(new_min_index, new_max_index);
  } else if (new_min_index >= offset && new_max_index < offset + static_cast<size_type>(bins.size())) {
    min_index = new_min_index;
    max_index = new_max_index;
  } else {
    // To avoid shifting too often when nearing the capacity of the array, we may grow it before
    // we actually reach the capacity.
    const size_type new_length = get_new_length(new_min_index, new_max_index);
    if (new_length > static_cast<size_type>(bins.size())) {
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
    std::fill(bins.begin() + min_arr_index, bins.begin() + shift, 0);
  } else {
    // std::move_backward(bins.begin() + min_arr_index, bins.begin() + max_arr_index + 1, bins.begin() + min_arr_index + shift);
    // std::fill(bins.begin() + max_arr_index + 1 + shift, bins.begin() + max_arr_index + 1, 0);

    std::move_backward(bins.begin() + min_arr_index, bins.begin() + max_arr_index + 1, bins.begin() + max_arr_index + shift + 1);
    std::fill(bins.begin() + max_arr_index + 1 + shift, bins.begin() + max_arr_index + 1, 0);
  }

  offset -= shift;
}

template<typename Allocator>
void DenseStore<Allocator>::center_bins(size_type new_min_index, size_type new_max_index) {
  const size_type middle_index = new_min_index + (new_max_index - new_min_index + 1) / 2;
  shift_bins(offset + bins.size() / 2 - middle_index);

  min_index = new_min_index;
  max_index = new_max_index;
}

template<typename Allocator>
typename DenseStore<Allocator>::size_type DenseStore<Allocator>::get_new_length(size_type new_min_index, size_type new_max_index) const {
  const size_type desired_length = new_max_index - new_min_index + 1;
  return ((desired_length + array_length_overhead - 1) / array_length_growth_increment + 1) * array_length_growth_increment;
}

template<typename Allocator>
void DenseStore<Allocator>::reset_bins() {
  reset_bins(min_index, max_index);
}

template<typename Allocator>
void DenseStore<Allocator>::reset_bins(size_type from_index, size_type to_index) {
  std::fill(bins.begin() + from_index - offset, bins.begin() + to_index - offset + 1, 0);
}

template<typename Allocator>
DenseStore<Allocator>::iterator::iterator(const bins_type& bins, size_type index, const size_type& max_index, const size_type& offset):
bins(bins),
index(index),
max_index(max_index),
offset(offset)
{}

template<typename Allocator>
typename DenseStore<Allocator>::iterator& DenseStore<Allocator>::iterator::operator++() {
  do {
    ++this->index;
  } while (this->index <= this->max_index && this->bins[this->index - this->offset] == 0);
  return *this;
}

template<typename Allocator>
bool DenseStore<Allocator>::iterator::operator!=(const iterator& other) const {
  return this->index != other.index;
}

template<typename Allocator>
typename DenseStore<Allocator>::iterator::reference DenseStore<Allocator>::iterator::operator*() const {
  return Bin(this->index, this->bins[this->index - this->offset]);
}

template<typename Allocator>
DenseStore<Allocator>::reverse_iterator::reverse_iterator(const bins_type& bins, size_type index, const size_type& min_index, const size_type& offset):
bins(bins),
index(index),
min_index(min_index),
offset(offset)
{}

template<typename Allocator>
typename DenseStore<Allocator>::reverse_iterator& DenseStore<Allocator>::reverse_iterator::operator++() {
  do {
    --this->index;
  } while (this->index >= this->min_index && this->bins[this->index - this->offset] == 0);
  return *this;
}

template<typename Allocator>
bool DenseStore<Allocator>::reverse_iterator::operator!=(const reverse_iterator& other) const {
  return this->index != other.index;
}

template<typename Allocator>
typename DenseStore<Allocator>::reverse_iterator::reference DenseStore<Allocator>::reverse_iterator::operator*() const {
  return Bin(this->index, this->bins[this->index - this->offset]);
}
}

#endif //DENSE_STORE_IMPL_HPP
