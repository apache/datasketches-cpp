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
bool SparseStore<Allocator>::operator==(const SparseStore &other) const {
  return bins == other.bins;
}

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
  if (bin.get_count() == 0) {
     return;
  }
  add(bin.get_index(), bin.get_count());
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
template<class OtherDerived>
void SparseStore<Allocator>::merge(const DenseStore<OtherDerived, Allocator> &other) {
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
typename SparseStore<Allocator>::iterator SparseStore<Allocator>::iterator::operator++(int) {
  iterator temp = *this;
  ++(*this);
  return temp;
}

template<typename Allocator>
typename SparseStore<Allocator>::iterator& SparseStore<Allocator>::iterator::operator=(const iterator& other) {
  if (this != &other) {
    this->it = other.it;
  }
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

//-----------------

template<typename Allocator>
typename SparseStore<Allocator>::reverse_iterator SparseStore<Allocator>::rbegin() const {
  return reverse_iterator(bins.rbegin());
}

template<typename Allocator>
typename SparseStore<Allocator>::reverse_iterator SparseStore<Allocator>::rend() const {
  return reverse_iterator(bins.rend());
}

template<typename Allocator>
SparseStore<Allocator>::reverse_iterator::reverse_iterator(internal_iterator it): it(it) {}

template<typename Allocator>
typename SparseStore<Allocator>::reverse_iterator& SparseStore<Allocator>::reverse_iterator::operator++() {
  ++it;
  return *this;
}

template<typename Allocator>
typename SparseStore<Allocator>::reverse_iterator SparseStore<Allocator>::reverse_iterator::operator++(int) {
  iterator temp = *this;
  ++(*this);
  return temp;
}

template<typename Allocator>
typename SparseStore<Allocator>::reverse_iterator& SparseStore<Allocator>::reverse_iterator::operator=(const reverse_iterator& other) {
  if (this != &other) {
    this->it = other.it;
  }
  return *this;
}

template<typename Allocator>
bool SparseStore<Allocator>::reverse_iterator::operator!=(const reverse_iterator& other) const {
  return it != other.it;
}

template<typename Allocator>
typename SparseStore<Allocator>::reverse_iterator::reference SparseStore<Allocator>::reverse_iterator::operator*() const {
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

template<typename Allocator>
void SparseStore<Allocator>::serialize(std::ostream &os) const {
  write(os, bins.size());
  for (const auto& [index, count] : bins) {
    write(os, index);
    write(os, count);
  }
}

template<typename Allocator>
SparseStore<Allocator> SparseStore<Allocator>::deserialize(std::istream& is) {
  SparseStore<Allocator> store;
  const auto num_bins = read<typename bins_type::size_type>(is);
  for (typename bins_type::size_type i = 0; i < num_bins; ++i) {
    const auto index = read<typename bins_type::key_type>(is);
    const auto count = read<typename bins_type::mapped_type>(is);
    store.bins[index] = count;
  }

  return store;
}

template<typename Allocator>
int SparseStore<Allocator>::get_serialized_size_bytes() const {
  int size_bytes = 0;
  size_bytes += sizeof(typename SparseStore<Allocator>::bins_type::size_type);
  size_bytes += bins.size() * sizeof(typename SparseStore<Allocator>::bins_type::key_type);
  size_bytes += bins.size() * sizeof(typename SparseStore<Allocator>::bins_type::mapped_type);

  return size_bytes;
}

template<typename Allocator>
string<Allocator> SparseStore<Allocator>::to_string() const {
  std::ostringstream os;
  os << "      Type              : sparse store" << std::endl;
  os << "      Bins number       : " << bins.size() << std::endl;
  return os.str();
}


}

#endif //SPARSE_STORE_IMPL_HPP