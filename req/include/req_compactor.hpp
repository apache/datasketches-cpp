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
  req_compactor(uint8_t lg_weight, uint32_t section_size, const Allocator& allocator, bool sorted = true);

  bool is_sorted() const;
  uint32_t get_num_items() const;
  uint32_t get_nom_capacity() const;
  uint8_t get_lg_weight() const;
  std::vector<T, Allocator>& get_items();
  const std::vector<T, Allocator>& get_items() const;

  template<bool inclusive>
  uint64_t compute_weight(const T& item) const;

  template<typename FwdT>
  void append(FwdT&& item);

  template<typename FwdC>
  void merge(FwdC&& other);

  void sort();
  void merge_sort_in(std::vector<T, Allocator>&& items);

  void compact(req_compactor& next);

  /**
   * Computes size needed to serialize the current state of the compactor.
   * This version is for fixed-size arithmetic types (integral and floating point).
   * @return size in bytes needed to serialize this compactor
   */
  template<typename S, typename TT = T, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type = 0>
  size_t get_serialized_size_bytes(const S& serde) const;

  /**
   * Computes size needed to serialize the current state of the compactor.
   * This version is for all other types and can be expensive since every item needs to be looked at.
   * @return size in bytes needed to serialize this compactor
   */
  template<typename S, typename TT = T, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type = 0>
  size_t get_serialized_size_bytes(const S& serde) const;

  template<typename S>
  void serialize(std::ostream& os, const S& serde) const;

  template<typename S>
  size_t serialize(void* dst, size_t capacity, const S& serde) const;

  template<typename S>
  static req_compactor deserialize(std::istream& is, const S& serde, const Allocator& allocator, bool sorted);

  template<typename S>
  static std::pair<req_compactor, size_t> deserialize(const void* bytes, size_t size, const S& serde, const Allocator& allocator, bool sorted);

  template<typename S>
  static req_compactor deserialize(std::istream& is, const S& serde, const Allocator& allocator, bool sorted, uint16_t k, uint8_t num_items);

  template<typename S>
  static std::pair<req_compactor, size_t> deserialize(const void* bytes, size_t size, const S& serde, const Allocator& allocator, bool sorted, uint16_t k, uint8_t num_items);

  template<typename S>
  static std::vector<T, Allocator> deserialize_items(std::istream& is, const S& serde, const Allocator& allocator, size_t num);

  template<typename S>
  static std::pair<std::vector<T, Allocator>, size_t> deserialize_items(const void* bytes, size_t size, const S& serde, const Allocator& allocator, size_t num);

private:
  uint8_t lg_weight_;
  bool coin_; // random bit for compaction
  bool sorted_;
  float section_size_raw_;
  uint32_t section_size_;
  uint8_t num_sections_;
  uint64_t state_; // state of the deterministic compaction schedule
  std::vector<T, Allocator> items_;

  bool ensure_enough_sections();
  size_t compute_compaction_range(uint32_t secs_to_compact) const;

  static uint32_t nearest_even(float value);

  template<typename InIter, typename OutIter>
  void promote_evens_or_odds(InIter from, InIter to, bool flag, OutIter dst);

  // for deserialization
  req_compactor(uint8_t lg_weight, bool sorted, float section_size_raw, uint8_t num_sections, uint64_t state, std::vector<T, Allocator>&& items);
};

} /* namespace datasketches */

#include "req_compactor_impl.hpp"

#endif
