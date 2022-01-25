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

#ifndef QUANTILE_SKETCH_SORTED_VIEW_HPP_
#define QUANTILE_SKETCH_SORTED_VIEW_HPP_

#include <functional>

namespace datasketches {

template<
  typename T,
  typename Comparator, // strict weak ordering function (see C++ named requirements: Compare)
  typename Allocator
>
class quantile_sketch_sorted_view {
public:
  using Entry = std::pair<const T*, uint64_t>;
  using AllocEntry = typename std::allocator_traits<Allocator>::template rebind_alloc<Entry>;
  using Container = std::vector<Entry, AllocEntry>;

  quantile_sketch_sorted_view(const Allocator& allocator);

  template<typename Iterator>
  void add(Iterator begin, Iterator end, uint64_t weight);

  template<bool inclusive>
  void convert_to_cummulative();

  class const_iterator;
  const_iterator begin() const;
  const_iterator end() const;

  size_t size() const;

  // makes sense only with cumulative weight
  const T& get_quantile(double rank) const;

  template<typename C>
  struct compare_pairs_by_first_ptr {
    bool operator()(const Entry& a, const Entry& b) {
      return C()(*a.first, *b.first);
    }
  };

  struct compare_pairs_by_second {
    bool operator()(const Entry& a, const Entry& b) {
      return a.second < b.second;
    }
  };

  uint64_t total_weight_;
  Container entries_;
};

template<typename T, typename C, typename A>
class quantile_sketch_sorted_view<T, C, A>::const_iterator: public quantile_sketch_sorted_view<T, C, A>::Container::const_iterator {
public:
  using Base = typename quantile_sketch_sorted_view<T, C, A>::Container::const_iterator;
  using value_type = std::pair<const T&, const uint64_t>;

  const_iterator(const Base& it): Base(it) {}

  const value_type operator*() const { return value_type(*(Base::operator*().first), Base::operator*().second); }

  class return_value_holder {
  public:
    return_value_holder(value_type value): value_(value) {}
    const value_type* operator->() const { return &value_; }
  private:
    value_type value_;
  };

  return_value_holder operator->() const { return **this; }
};

} /* namespace datasketches */

#include "quantile_sketch_sorted_view_impl.hpp"

#endif