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

#ifndef DENSE_STORE_HPP
#define DENSE_STORE_HPP

#include <vector>
#include "bin.hpp"

namespace datasketches {
// Forward declaration
template<typename Allocator> class SparseStore;

template<class Derived, typename Allocator>
class DenseStore {
public:
  using bins_type = std::vector<double, typename std::allocator_traits<Allocator>::template rebind_alloc<double>>;
  using size_type = int;
  class iterator;
  class reverse_iterator;

  DenseStore();
  explicit DenseStore(const int& array_length_growth_increment);
  explicit DenseStore(const int& array_length_growth_increment, const int& array_length_overhead);
  DenseStore(const DenseStore& other) = default;

  void add(int index);
  void add(int index, double count);
  void add(const Bin& bin);

  DenseStore<Derived, Allocator>* copy();

  void clear();
  bool is_empty() const;
  size_type get_max_index() const;
  size_type get_min_index() const;
  double get_total_count() const;

  template<class Store>
  void merge(const DenseStore<Store, Allocator>& other);

  void merge(const SparseStore<Allocator>& other);

  iterator begin() const;
  iterator end() const;

  reverse_iterator rbegin() const;
  reverse_iterator rend() const;

  ~DenseStore() = default;

  class iterator {
  public:
    using iterator_category = std::input_iterator_tag;
    using value_type = Bin;
    using difference_type = std::ptrdiff_t;
    using pointer = Bin*;
    using reference = Bin;

    iterator(const bins_type& bins, const size_type& index, const size_type& max_index, const size_type& offset);
    iterator& operator=(const iterator& other);
    iterator& operator++();
    iterator operator++(int);
    bool operator!=(const iterator& other) const;
    reference operator*() const;

  private:
    const bins_type& bins;
    size_type index;
    const size_type& max_index;
    const size_type& offset;
  };

  class reverse_iterator {
  public:
    using iterator_category = std::input_iterator_tag;
    using value_type = Bin;
    using difference_type = std::ptrdiff_t;
    using pointer = Bin*;
    using reference = Bin;

    reverse_iterator(const bins_type& bins, size_type index, const size_type& min_index, const size_type& offset);
    reverse_iterator& operator=(const reverse_iterator& other);
    reverse_iterator& operator++();
    bool operator!=(const reverse_iterator& other) const;
    reference operator*() const;

  private:
    const bins_type& bins;
    size_type index;
    const size_type& min_index;
    const size_type& offset;
  };

protected:
  bins_type bins;
  size_type offset;
  size_type min_index;
  size_type max_index;

  const int array_length_growth_increment;
  const int array_length_overhead;

  static constexpr int DEFAULT_ARRAY_LENGTH_GROWTH_INCREMENT = 64;
  static constexpr double DEFAULT_ARRAY_LENGTH_OVERHEAD_RATIO = 0.1;

  double get_total_count(size_type from_index, size_type to_index) const;
  size_type normalize(size_type index);
  void adjust(size_type newMinIndex, size_type newMaxIndex);
  void extend_range(size_type index);
  void extend_range(size_type new_min_index, size_type new_max_index);
  void shift_bins(size_type shift);
  void center_bins(size_type new_min_index, size_type new_max_index);
  size_type get_new_length(size_type new_min_index, size_type new_max_index) const;
  void reset_bins();
  void reset_bins(size_type from_index, size_type to_index);

  Derived& derived();
  const Derived& derived() const;
};
}

#include "dense_store_impl.hpp"

#endif //DENSE_STORE_HPP
