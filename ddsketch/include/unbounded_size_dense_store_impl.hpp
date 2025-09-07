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

#ifndef UNBOUNDED_SIZE_DENSE_STORE_IMPL_HPP
#define UNBOUNDED_SIZE_DENSE_STORE_IMPL_HPP

#include "unbounded_size_dense_store.hpp"

namespace datasketches {
template<typename Allocator>
UnboundedSizeDenseStore<Allocator>::UnboundedSizeDenseStore(): DenseStore<UnboundedSizeDenseStore<Allocator>, Allocator>() {}

template<typename Allocator>
UnboundedSizeDenseStore<Allocator>::UnboundedSizeDenseStore(const int &array_length_growth_increment): DenseStore<UnboundedSizeDenseStore<Allocator>, Allocator>(array_length_growth_increment) {}

template<typename Allocator>
UnboundedSizeDenseStore<Allocator>::UnboundedSizeDenseStore(const int &array_length_growth_increment, const int &array_length_overhead): DenseStore<UnboundedSizeDenseStore<Allocator>, Allocator>(array_length_growth_increment, array_length_overhead) {}

template<typename Allocator>
typename UnboundedSizeDenseStore<Allocator>::size_type UnboundedSizeDenseStore<Allocator>::normalize(size_type index) {
  if (index < this->min_index || index > this->max_index) {
    this->extend_range(index);
  }

  return index - this->offset;
}

template<typename Allocator>
UnboundedSizeDenseStore<Allocator> *UnboundedSizeDenseStore<Allocator>::copy() const {
  using StoreAlloc = typename std::allocator_traits<Allocator>::template rebind_alloc<UnboundedSizeDenseStore<Allocator>>;
  StoreAlloc alloc(this->bins.get_allocator());
  return new (alloc.allocate(1)) UnboundedSizeDenseStore<Allocator>(*this);
}


template<typename Allocator>
void UnboundedSizeDenseStore<Allocator>::adjust(size_type new_min_index, size_type new_max_index) {
  this->center_bins(new_min_index, new_max_index);
}

template<typename Allocator>
void UnboundedSizeDenseStore<Allocator>::merge(const UnboundedSizeDenseStore<Allocator> &other) {
  if (other.is_empty()) {
    return;
  }

  if (other.get_min_index() < this->min_index || other.get_max_index() > this->max_index) {
    this->extend_range(other.get_min_index(), other.get_max_index());
  }

  for (int index = other.get_min_index(); index <= other.get_max_index(); ++index) {
    this->bins[index - this->offset] += other.bins[index - other.offset];
  }
}



}
#endif //UNBOUNDED_SIZE_DENSE_STORE_IMPL_HPP
