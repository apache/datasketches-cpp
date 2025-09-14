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

#ifndef COLLAPSING_DENSE_STORE_IMPL_HPP
#define COLLAPSING_DENSE_STORE_IMPL_HPP

#include "collapsing_dense_store.hpp"

namespace datasketches {
template<class Derived, int N, typename Allocator>
CollapsingDenseStore<Derived, N, Allocator>::CollapsingDenseStore():
  DenseStore<Derived, Allocator>(),
  is_collapsed(false) {}

template<class Derived, int N, typename Allocator>
CollapsingDenseStore<Derived, N, Allocator>& CollapsingDenseStore<Derived, N, Allocator>::operator=(const CollapsingDenseStore<Derived, N, Allocator>& other) {
  this->bins = other.bins;
  this->offset = other.offset;
  this->min_index = other.min_index;
  this->max_index = other.max_index;


  return *this;
}

template<class Derived, int N, typename Allocator>
typename CollapsingDenseStore<Derived, N, Allocator>::size_type CollapsingDenseStore<Derived, N, Allocator>::get_new_length(size_type new_min_index, size_type new_max_index) const {
  return std::min(DenseStore<Derived, Allocator>::get_new_length(new_min_index, new_max_index), N);
}

template<class Derived, int N, typename Allocator>
void CollapsingDenseStore<Derived, N, Allocator>::clear() {
  DenseStore<Derived, Allocator>::clear();
  is_collapsed = false;
}

template<class Derived, int N, typename Allocator>
void CollapsingDenseStore<Derived, N, Allocator>::serialize(std::ostream& os) const {
  if (this->is_empty()) {
    return;
  }
  write(os, is_collapsed);

  this->serialize_common(os);
}

template<class Derived, int N, typename Allocator>
Derived CollapsingDenseStore<Derived, N, Allocator>::deserialize(std::istream& is) {
  Derived store;

  if (is.peek() == std::istream::traits_type::eof()) {
    return store;
  }
  store.is_collapsed = read<bool>(is);
  Derived::deserialize_common(store, is);

  return store;
}

template<class Derived, int N, typename Allocator>
int CollapsingDenseStore<Derived, N, Allocator>::get_serialized_size_bytes() const {
  // Header written by serialize(): max_num_bins always present
  int size_bytes = 0;
  if (this->is_empty()) {
    return size_bytes;
  }
  // is_collapsed flag, then the common section (range + bins)
  size_bytes += static_cast<int>(sizeof(is_collapsed));
  size_bytes += this->get_serialized_size_bytes_common();
  return size_bytes;
}

}
#endif //COLLAPSING_DENSE_STORE_IMPL_HPP
