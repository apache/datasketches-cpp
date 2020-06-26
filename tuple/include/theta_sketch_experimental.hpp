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

#ifndef THETA_SKETCH_EXPERIMENTAL_HPP_
#define THETA_SKETCH_EXPERIMENTAL_HPP_

#include "serde.hpp"
#include "theta_update_sketch_base.hpp"

namespace datasketches {

// experimental theta sketch derived from the same base as tuple sketch

template<typename A> class compact_theta_sketch_experimental;

template<typename A = std::allocator<uint64_t>>
class theta_sketch_experimental {
public:
  using resize_factor = theta_constants::resize_factor;
  using AllocBytes = typename std::allocator_traits<A>::template rebind_alloc<uint8_t>;
  using vector_bytes = std::vector<uint8_t, AllocBytes>;

  class builder: public theta_base_builder<builder> {
  public:
      theta_sketch_experimental build() const;
  };

  bool is_empty() const { return table_.is_empty_; }
  bool is_ordered() const { return false; }
  uint16_t get_seed_hash() const { return compute_seed_hash(DEFAULT_SEED); }
  uint64_t get_theta64() const { return table_.theta_; }
  uint32_t get_num_retained() const { return table_.num_entries_; }

  void update(uint64_t key);
  void update(const void* key, size_t length);

  void trim();

  string<A> to_string(bool detail = false) const;

  vector_bytes serialize(unsigned header_size_bytes = 0) const;

  using const_iterator = theta_const_iterator<uint64_t, trivial_extract_key<uint64_t>>;
  const_iterator begin() const;
  const_iterator end() const;

  compact_theta_sketch_experimental<A> compact(bool ordered = true) const;

private:
  enum flags { IS_BIG_ENDIAN, IS_READ_ONLY, IS_EMPTY, IS_COMPACT, IS_ORDERED };
  using theta_table = theta_update_sketch_base<uint64_t, trivial_extract_key<uint64_t>, A>;
  theta_table table_;

  theta_sketch_experimental(uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf, float p, uint64_t seed);
};

template<typename A = std::allocator<uint64_t>>
class compact_theta_sketch_experimental {
public:
  compact_theta_sketch_experimental(const theta_sketch_experimental<A>& other, bool ordered);

  template<typename InputIt>
  compact_theta_sketch_experimental(bool is_empty, bool is_ordered, uint16_t seed_hash, uint64_t theta, InputIt first, InputIt last);

  compact_theta_sketch_experimental(bool is_empty, bool is_ordered, uint16_t seed_hash, uint64_t theta, std::vector<uint64_t, A>&& entries);

  uint32_t get_num_retained() const { return entries_.size(); }

  string<A> to_string(bool detail = false) const;

private:
  bool is_empty_;
  bool is_ordered_;
  uint16_t seed_hash_;
  uint64_t theta_;
  std::vector<uint64_t, A> entries_;
};

} /* namespace datasketches */

#include "theta_sketch_experimental_impl.hpp"

#endif
