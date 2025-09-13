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
template<class Derived, typename Allocator>
CollapsingDenseStore<Derived, Allocator>::CollapsingDenseStore(size_type max_num_bins): DenseStore<Derived, Allocator>(), max_num_bins(max_num_bins), is_collapsed(false) {}

template<class Derived, typename Allocator>
CollapsingDenseStore<Derived, Allocator>& CollapsingDenseStore<Derived, Allocator>::operator=(const CollapsingDenseStore<Derived, Allocator>& other) {
  if (max_num_bins != other.max_num_bins) {
    throw std::runtime_error("stores must have same maximum number of bins");
  }

  this->bins = other.bins;
  this->offset = other.offset;
  this->min_index = other.min_index;
  this->max_index = other.max_index;


  return *this;
}

template<class Derived, typename Allocator>
typename CollapsingDenseStore<Derived, Allocator>::size_type CollapsingDenseStore<Derived, Allocator>::get_new_length(size_type new_min_index, size_type new_max_index) const {
  return std::min(DenseStore<Derived, Allocator>::get_new_length(new_min_index, new_max_index), max_num_bins);
}

template<class Derived, typename Allocator>
void CollapsingDenseStore<Derived, Allocator>::clear() {
  DenseStore<Derived, Allocator>::clear();
  is_collapsed = false;
}

template<class Derived, typename Allocator>
void CollapsingDenseStore<Derived, Allocator>::serialize(std::ostream& os) const {
  write(os, max_num_bins);
  if (this->is_empty()) {
    return;
  }
  write(os, is_collapsed);

  this->serialize_common(os);
}

template<class Derived, typename Allocator>
Derived CollapsingDenseStore<Derived, Allocator>::deserialize(std::istream& is) {
  Derived store(read<int>(is));

  if (is.peek() == std::istream::traits_type::eof()) {
    return store;
  }
  store.is_collapsed = read<bool>(is);
  Derived::deserialize_common(store, is);

  return store;
}

}
#endif //COLLAPSING_DENSE_STORE_IMPL_HPP
