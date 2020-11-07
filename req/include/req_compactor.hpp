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

#ifndef REQ_COMPACTOR_HPP_
#define REQ_COMPACTOR_HPP_

namespace datasketches {

template<
typename T,
bool IsHighRank,
typename Comparator,
typename Allocator
>
class req_compactor {
public:
  req_compactor(uint8_t lg_weight, uint32_t section_size, const Allocator& allocator);

  bool is_sorted() const;
  uint32_t get_num_items() const;
  uint32_t get_nom_capacity() const;
  uint8_t get_lg_weight() const;
  const std::vector<T, Allocator>& get_items() const;

  template<bool inclusive>
  uint64_t compute_weight(const T& item) const;

  template<typename FwdT>
  void append(FwdT&& item);

  void sort();
  void merge_sort_in(std::vector<T, Allocator>&& items);

  std::vector<T, Allocator> compact();

  template<typename S>
  void serialize(std::ostream& os, const S& serde) const;

  template<typename S>
  static req_compactor deserialize(std::istream& is, const S& serde, const Allocator& allocator, bool sorted);

  // for deserialization
  req_compactor(uint8_t lg_weight, bool coin, bool sorted, double section_size_raw, uint32_t num_sections, uint32_t num_compactions, uint32_t state, std::vector<T, Allocator>&& items);

private:
  uint8_t lg_weight_;
  bool coin_; // random bit for compaction
  bool sorted_;
  double section_size_raw_;
  uint32_t section_size_;
  uint32_t num_sections_;
  uint32_t num_compactions_;
  uint32_t state_; // state of the deterministic compaction schedule
  std::vector<T, Allocator> items_;

  bool ensure_enough_sections();
  size_t compute_compaction_range(uint32_t secs_to_compact) const;

  static uint32_t nearest_even(double value);

  template<typename Iter>
  static std::vector<T, Allocator> get_evens_or_odds(Iter from, Iter to, bool flag);
};

} /* namespace datasketches */

#include "req_compactor_impl.hpp"

#endif
