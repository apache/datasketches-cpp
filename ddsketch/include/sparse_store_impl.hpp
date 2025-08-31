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

#ifndef SPARSE_STORE_IMPL_HPP
#define SPARSE_STORE_IMPL_HPP

#include "sparse_store.hpp"

namespace datasketches {
template<typename Allocator>
void SparseStore<Allocator>::add(int index) {
  add(index, 1);
}

template<typename Allocator>
void SparseStore<Allocator>::add(int index, double count) {
  if (count == 0) {
     return;
  }
  bins[index] += count;
}

template<typename Allocator>
void SparseStore<Allocator>::add(const Bin &bin) {
  if (bin.getCount() == 0) {
     return;
  }
  add(bin.getIndex(), bin.getCount());
}

template<typename Allocator>
SparseStore<Allocator>* SparseStore<Allocator>::copy() const {
  using SparseStoreAlloc = typename std::allocator_traits<Allocator>::template rebind_alloc<SparseStore<Allocator>>;
  SparseStoreAlloc alloc(this->bins.get_allocator());
  return new (alloc.allocate(1)) SparseStore<Allocator>(*this);
}

template<typename Allocator>
void SparseStore<Allocator>::clear() {
  bins.clear();
}

template<typename Allocator>
int SparseStore<Allocator>::get_min_index() const {
  if (bins.empty()) {
     throw std::runtime_error("operation is undefined for an empty sparse store");
  }
  return bins.begin()->first;
}

template<typename Allocator>
int SparseStore<Allocator>::get_max_index() const {
  if (bins.empty()) {
     throw std::runtime_error("operation is undefined for an empty sparse store");
  }
  return bins.rbegin()->first;
}

template<typename Allocator>
void SparseStore<Allocator>::merge(const SparseStore<Allocator>& other) {
  for (const Bin &bin : other) {
    add(bin);
  }
}

template<typename Allocator>
void SparseStore<Allocator>::merge(const DenseStore<Allocator> &other) {
  for (const Bin& bin : other) {
    add(bin);
  }
}

template<typename Allocator>
bool SparseStore<Allocator>::is_empty() const {
  return bins.empty();
}

template<typename Allocator>
typename SparseStore<Allocator>::iterator SparseStore<Allocator>::begin() const {
  return iterator(bins.begin());
}

template<typename Allocator>
typename SparseStore<Allocator>::iterator SparseStore<Allocator>::end() const {
  return iterator(bins.end());
}

template<typename Allocator>
SparseStore<Allocator>::iterator::iterator(internal_iterator it): it(it) {}

template<typename Allocator>
typename SparseStore<Allocator>::iterator& SparseStore<Allocator>::iterator::operator++() {
  ++it;
  return *this;
}

template<typename Allocator>
bool SparseStore<Allocator>::iterator::operator!=(const iterator& other) const {
  return it != other.it;
}

template<typename Allocator>
typename SparseStore<Allocator>::iterator::reference SparseStore<Allocator>::iterator::operator*() const {
  return Bin(it->first, it->second);
}

template<typename Allocator>
double SparseStore<Allocator>::get_total_count() const {
  double total_count = 0;
  for (typename bins_type::const_iterator it = bins.begin(); it != bins.end(); ++it) {
    total_count += it->second;
  }
  return total_count;
}
}

#endif //SPARSE_STORE_IMPL_HPP
