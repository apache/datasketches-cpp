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

#ifndef REQ_QUANTILE_CALCULATOR_HPP_
#define REQ_QUANTILE_CALCULATOR_HPP_

#include <functional>

namespace datasketches {

template<
  typename T,
  typename Comparator,
  typename Allocator
>
class req_quantile_calculator {
public:
  req_quantile_calculator(uint64_t n, const Allocator& allocator);

  void add(const T* begin, const T* end, uint8_t lg_weight);

  template<bool inclusive>
  void convert_to_cummulative();

  const T* get_quantile(double rank) const;

private:
  using Entry = std::pair<const T*, uint64_t>;
  using AllocEntry = typename std::allocator_traits<Allocator>::template rebind_alloc<Entry>;
  using Container = std::vector<Entry, AllocEntry>;

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

  uint64_t n_;
  Container entries_;
};

} /* namespace datasketches */

#include "req_quantile_calculator_impl.hpp"

#endif
