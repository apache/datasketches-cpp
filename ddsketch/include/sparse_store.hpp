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

#ifndef SPARSE_STORE_HPP
#define SPARSE_STORE_HPP

#include <map>

#include "bin.hpp"


namespace datasketches {
// Forward declaration
template<class Derived, typename Allocator> class DenseStore;

template<typename Allocator>
class SparseStore {
public:
  using bins_type = std::map<
      int,
      double,
      std::less<int>,
      typename std::allocator_traits<Allocator>::template rebind_alloc<std::pair<const int, double>>
  >;
  class iterator;
  class reverse_iterator;

  SparseStore() = default;

  bool operator==(const SparseStore &other) const;

  void add(int index);
  void add(int index, double count);
  void add(const Bin& bin);
  SparseStore<Allocator>* copy() const;
  void clear();
  int get_min_index() const;
  int get_max_index() const;
  void merge(const SparseStore<Allocator>& other);

  template<class Derived>
  void merge(const DenseStore<Derived, Allocator>& other);
  bool is_empty() const;
  double get_total_count() const;

  void serialize(std::ostream& os) const;
  static SparseStore deserialize(std::istream& is);
  int get_serialized_size_bytes() const;

  string<Allocator> to_string() const;

  iterator begin() const;
  iterator end() const;
  reverse_iterator rbegin() const;
  reverse_iterator rend() const;

  class iterator {
  public:
    using internal_iterator = typename bins_type::const_iterator;
    using iterator_category = std::input_iterator_tag;
    using value_type = Bin;
    using difference_type = std::ptrdiff_t;
    using pointer = Bin*;
    using reference = Bin;

    explicit iterator(internal_iterator it);
    iterator& operator++();
    iterator operator++(int);
    iterator& operator=(const iterator& other);
    bool operator!=(const iterator& other) const;
    reference operator*() const;

  private:
    internal_iterator it;
  };

  class reverse_iterator {
    public:
    using internal_iterator = typename bins_type::const_reverse_iterator;
    using iterator_category = std::input_iterator_tag;
    using value_type = Bin;
    using difference_type = std::ptrdiff_t;
    using pointer = Bin*;
    using reference = Bin;

    explicit reverse_iterator(internal_iterator it);
    reverse_iterator& operator++();
    reverse_iterator operator++(int);
    reverse_iterator& operator=(const reverse_iterator& other);
    bool operator!=(const reverse_iterator& other) const;
    reference operator*() const;

  private:
    internal_iterator it;
  };


private:
  bins_type bins;
};
}

#include "sparse_store_impl.hpp"

#endif //SPARSE_STORE_HPP
