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
#include <filesystem>

#include "common_defs.hpp"

namespace datasketches {

template<class Derived, typename Allocator>
DenseStore<Derived, Allocator>::DenseStore() :
  DenseStore(DEFAULT_ARRAY_LENGTH_GROWTH_INCREMENT)
{}

template<class Derived, typename Allocator>
DenseStore<Derived, Allocator>::DenseStore(const int& array_length_growth_increment) :
  DenseStore(array_length_growth_increment, array_length_growth_increment * DEFAULT_ARRAY_LENGTH_OVERHEAD_RATIO)
{}

template<class Derived, typename Allocator>
DenseStore<Derived, Allocator>::DenseStore(const int& array_length_growth_increment, const int& array_length_overhead):
  offset(0),
  min_index(std::numeric_limits<size_type>::max()),
  max_index(std::numeric_limits<size_type>::min()),
  array_length_growth_increment(array_length_growth_increment),
  array_length_overhead(array_length_overhead)
{}

template<class Derived, typename Allocator>
void DenseStore<Derived, Allocator>::add(int index) {
  add(index, 1);
}

template<class Derived, typename Allocator>
void DenseStore<Derived, Allocator>::add(int index, double count) {
  if (count == 0) {
    return;
  }
  const size_type array_index = derived().normalize(index);
  bins[array_index] += count;
}

template<class Derived, typename Allocator>
void DenseStore<Derived, Allocator>::add(const Bin&  bin) {
  if (bin.getCount() == 0) {
    return;
  }
  add(bin.getIndex(), bin.getCount());
}

template<class Derived, typename Allocator>
void DenseStore<Derived, Allocator>::clear() {
  bins.clear();
  min_index = std::numeric_limits<size_type>::max();
  max_index = std::numeric_limits<size_type>::min();
  offset = 0;
}

template<class Derived, typename Allocator>
bool DenseStore<Derived, Allocator>::is_empty() const {
  // return get_total_count() == 0;
  return max_index < min_index;
}

template<class Derived, typename Allocator>
typename DenseStore<Derived, Allocator>::size_type DenseStore<Derived, Allocator>::get_max_index() const {
  if (is_empty()) {
    throw std::runtime_error("store is empty");
  }
  return max_index;
}

template<class Derived, typename Allocator>
typename DenseStore<Derived, Allocator>::size_type DenseStore<Derived, Allocator>::get_min_index() const {
  if (is_empty()) {
    throw std::runtime_error("store is empty");
  }
  return min_index;
}

template<class Derived, typename Allocator>
double DenseStore<Derived, Allocator>::get_total_count() const {
  return get_total_count(min_index, max_index);
}

template<class Derived, typename Allocator>
double DenseStore<Derived, Allocator>::get_total_count(size_type from_index, size_type to_index) const {
  if (is_empty()) {
    return 0;
  }

  double total_count = 0;
  size_type from_array_index = std::max(from_index - offset, static_cast<size_type>(0));
  size_type to_array_index = std::min(to_index - offset, static_cast<size_type>(bins.size() - 1));
  for (size_type index = from_array_index; index <= to_array_index; index++) {
    total_count += bins[index];
  }

  return total_count;
}


template<class Derived, typename Allocator>
template<class OtherDerived>
void DenseStore<Derived, Allocator>::merge(const DenseStore<OtherDerived, Allocator>& other) {
  for (const Bin& bin : other) {
    add(bin);
  }
}

template<class Derived, typename Allocator>
void DenseStore<Derived, Allocator>::merge(const SparseStore<Allocator>& other) {
  for (const Bin &bin : other) {
    add(bin);
  }
}

template<class Derived, typename Allocator>
typename DenseStore<Derived, Allocator>::iterator DenseStore<Derived, Allocator>::begin() const {
  if (is_empty()) {
    return end();
  }
  return DenseStore<Derived, Allocator>::iterator(this->bins, this->min_index, this->max_index, this->offset);
}

template<class Derived, typename Allocator>
typename DenseStore<Derived, Allocator>::iterator DenseStore<Derived, Allocator>::end() const {
  return DenseStore<Derived, Allocator>::iterator(this->bins, this->max_index + 1, this->max_index, this->offset);
}

template<class Derived, typename Allocator>
typename DenseStore<Derived, Allocator>::reverse_iterator DenseStore<Derived, Allocator>::rbegin() const {
  if (is_empty()) {
    return rend();
  }
  return DenseStore<Derived, Allocator>::reverse_iterator(this->bins, this->max_index, this->min_index, this->offset);
}

template<class Derived, typename Allocator>
typename DenseStore<Derived, Allocator>::reverse_iterator DenseStore<Derived, Allocator>::rend() const {
  return DenseStore<Derived, Allocator>::reverse_iterator(this->bins, this->min_index - 1, this->min_index, this->offset);
}


template<class Derived, typename Allocator>
typename DenseStore<Derived, Allocator>::size_type DenseStore<Derived, Allocator>::normalize(size_type index) {
  if (index < get_min_index() || index > get_max_index()) {
    extend_range(index, index);
  }
  return index - offset;
}

template<class Derived, typename Allocator>
void DenseStore<Derived, Allocator>::adjust(size_type newMinIndex, size_type newMaxIndex) {
  derived().adjust(newMinIndex, newMaxIndex);
}

template<class Derived, typename Allocator>
void DenseStore<Derived, Allocator>::extend_range(size_type index) {
  extend_range(index, index);
}

template<class Derived, typename Allocator>
void DenseStore<Derived, Allocator>::extend_range(size_type new_min_index, size_type new_max_index) {
  new_min_index = std::min(new_min_index, min_index);
  new_max_index = std::max(new_max_index, max_index);

  if (is_empty()) {
    const size_type initial_length = derived().get_new_length(new_min_index, new_max_index);
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
    const size_type new_length = derived().get_new_length(new_min_index, new_max_index);
    if (new_length > static_cast<size_type>(bins.size())) {
      bins.resize(new_length);
    }

    adjust(new_min_index, new_max_index);
  }
}

template<class Derived, typename Allocator>
void DenseStore<Derived, Allocator>::shift_bins(size_type shift) {
  const size_type min_arr_index = min_index - offset;
  const size_type max_arr_index = max_index - offset;

  std::copy(bins.begin() + min_arr_index, bins.begin() + max_arr_index + 1, bins.begin() + min_arr_index + shift);

  if (shift > 0) {
    std::fill(bins.begin() + min_arr_index, bins.begin() + min_arr_index + shift, 0);
  } else {
    std::fill(bins.begin() + max_arr_index + 1 + shift, bins.begin() + max_arr_index + 1, 0.);
  }

  offset -= shift;
}

template<class Derived, typename Allocator>
void DenseStore<Derived, Allocator>::center_bins(size_type new_min_index, size_type new_max_index) {
  const size_type middle_index = new_min_index + (new_max_index - new_min_index + 1) / 2;
  shift_bins(offset + bins.size() / 2 - middle_index);

  min_index = new_min_index;
  max_index = new_max_index;
}

template<class Derived, typename Allocator>
typename DenseStore<Derived, Allocator>::size_type DenseStore<Derived, Allocator>::get_new_length(size_type new_min_index, size_type new_max_index) const {
  const size_type desired_length = new_max_index - new_min_index + 1;
  return ((desired_length + array_length_overhead - 1) / array_length_growth_increment + 1) * array_length_growth_increment;
}

template<class Derived, typename Allocator>
void DenseStore<Derived, Allocator>::reset_bins() {
  reset_bins(min_index, max_index);
}

template<class Derived, typename Allocator>
void DenseStore<Derived, Allocator>::reset_bins(size_type from_index, size_type to_index) {
  std::fill(bins.begin() + from_index - offset, bins.begin() + to_index - offset + 1, 0);
}

template<class Derived, typename Allocator>
bool DenseStore<Derived, Allocator>::operator==(const DenseStore<Derived, Allocator>& other) const {
  return offset == other.offset &&
    min_index == other.min_index &&
    max_index == other.max_index &&
    bins == other.bins;
}

template<class Derived, typename Allocator>
Derived& DenseStore<Derived, Allocator>::derived() {
  return static_cast<Derived&>(*this);
}

template<class Derived, typename Allocator>
const Derived& DenseStore<Derived, Allocator>::derived() const {
  return static_cast<const Derived&>(*this);
}

template<class Derived, typename Allocator>
DenseStore<Derived, Allocator>::iterator::iterator(const bins_type& bins, const size_type& index, const size_type& max_index, const size_type& offset):
bins(bins),
index(index),
max_index(max_index),
offset(offset)
{}

template<class Derived, typename Allocator>
typename DenseStore<Derived, Allocator>::iterator& DenseStore<Derived, Allocator>::iterator::operator=(const iterator& other) {
  if (this != &other) {
    // Note: we can't assign to reference members, so we only copy the index
    // The reference members (bins, max_index, offset) should already point to the same objects
    this->index = other.index;
  }
  return *this;
}

template<class Derived, typename Allocator>
typename DenseStore<Derived, Allocator>::iterator& DenseStore<Derived, Allocator>::iterator::operator++() {
  do {
    ++this->index;
  } while (this->index <= this->max_index && this->bins[this->index - this->offset] == 0);
  return *this;
}

template<class Derived, typename Allocator>
typename DenseStore<Derived, Allocator>::iterator DenseStore<Derived, Allocator>::iterator::operator++(int) {
  iterator temp = *this;
  ++(*this);
  return temp;
}

template<class Derived, typename Allocator>
bool DenseStore<Derived, Allocator>::iterator::operator!=(const iterator& other) const {
  return this->index != other.index;
}

template<class Derived, typename Allocator>
typename DenseStore<Derived, Allocator>::iterator::reference DenseStore<Derived, Allocator>::iterator::operator*() const {
  return Bin(this->index, this->bins[this->index - this->offset]);
}

template<class Derived, typename Allocator>
DenseStore<Derived, Allocator>::reverse_iterator::reverse_iterator(const bins_type& bins, size_type index, const size_type& min_index, const size_type& offset):
bins(bins),
index(index),
min_index(min_index),
offset(offset)
{}

template<class Derived, typename Allocator>
typename DenseStore<Derived, Allocator>::reverse_iterator& DenseStore<Derived, Allocator>::reverse_iterator::operator=(const reverse_iterator& other) {
  if (this != &other) {
    // Note: we can't assign to reference members, so we only copy the index
    // The reference members (bins, min_index, offset) should already point to the same objects
    this->index = other.index;
  }
  return *this;
}

template<class Derived, typename Allocator>
typename DenseStore<Derived, Allocator>::reverse_iterator& DenseStore<Derived, Allocator>::reverse_iterator::operator++() {
  do {
    --this->index;
  } while (this->index >= this->min_index && this->bins[this->index - this->offset] == 0);
  return *this;
}

template<class Derived, typename Allocator>
bool DenseStore<Derived, Allocator>::reverse_iterator::operator!=(const reverse_iterator& other) const {
  return this->index != other.index;
}

template<class Derived, typename Allocator>
typename DenseStore<Derived, Allocator>::reverse_iterator::reference DenseStore<Derived, Allocator>::reverse_iterator::operator*() const {
  return Bin(this->index, this->bins[this->index - this->offset]);
}

template<class Derived, typename Allocator>
void DenseStore<Derived, Allocator>::serialize(std::ostream& os) const {
  derived().serialize(os);
}

template<class Derived, typename Allocator>
Derived DenseStore<Derived, Allocator>::deserialize(std::istream& is) {
  return Derived::deserialize(is);
}

template<class Derived, typename Allocator>
void DenseStore<Derived, Allocator>::serialize_common(std::ostream& os) const {
  if (is_empty()) {
    return;
  }

  // Serialize the range information
  write(os, min_index);
  write(os, max_index);
  write(os, offset);

  // Serialize the bins array (only the used portion)
  const size_type num_bins = bins.size();
  write(os, num_bins);

  size_type non_empty_bins = 0;
  for (const double& count : bins) {
    non_empty_bins += (count > 0.0);
  }
  write(os, non_empty_bins);

  for (const Bin& bin : *this) {
    write(os, bin.getIndex());
    write(os, bin.getCount());
  }
}

template<class Derived, typename Allocator>
void DenseStore<Derived, Allocator>::deserialize_common(Derived& store, std::istream& is) {
  if (is.peek() == std::istream::traits_type::eof()) {
    return;
  }
  // Deserialize the range information
  store.min_index = read<size_type>(is);
  store.max_index = read<size_type>(is);
  store.offset = read<size_type>(is);

  // Deserialize the bins array
  const auto num_bins = read<size_type>(is);
  store.bins.resize(num_bins, 0.0);

  const auto non_empty_bins = read<size_type>(is);
  // Read the actual bin counts
  for (size_type i = 0; i < non_empty_bins; ++i) {
    const auto index =  read<int>(is);
    const auto count = read<double>(is);
    store.add(index, count);
  }
}
}

#endif //DENSE_STORE_IMPL_HPP
