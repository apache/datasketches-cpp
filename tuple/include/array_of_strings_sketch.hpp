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

#ifndef ARRAY_OF_STRINGS_SKETCH_HPP_
#define ARRAY_OF_STRINGS_SKETCH_HPP_

#include <memory>
#include <string>

#include "array_tuple_sketch.hpp"
#include "xxhash64.h"

namespace datasketches {

// default update policy for an array of strings
template<typename Allocator = std::allocator<std::string>>
class default_array_of_strings_update_policy {
public:
  using array_of_strings = array<std::string, Allocator>;

  explicit default_array_of_strings_update_policy(const Allocator& allocator = Allocator());

  array_of_strings create() const;

  void update(array_of_strings& array, const array_of_strings& input) const;

  void update(array_of_strings& array, const array_of_strings* input) const;

private:
  Allocator allocator_;
};

// serializer/deserializer for an array of strings
// Requirements: all strings must be valid UTF-8 and array size must be <= 127.
template<typename Allocator = std::allocator<std::string>>
struct array_of_strings_serde {
  using array_of_strings = array<std::string, Allocator>;

  void serialize(std::ostream& os, const array_of_strings* items, unsigned num) const;
  void deserialize(std::istream& is, array_of_strings* items, unsigned num) const;
  size_t serialize(void* ptr, size_t capacity, const array_of_strings* items, unsigned num) const;
  size_t deserialize(const void* ptr, size_t capacity, array_of_strings* items, unsigned num) const;
  size_t size_of_item(const array_of_strings& item) const;

private:
  static void check_num_nodes(uint8_t num_nodes);
  static uint32_t compute_total_bytes(const array_of_strings& item);
  static void check_utf8(const std::string& value);
};

/**
 * Extended class of compact_tuple_sketch for array of strings
 * Requirements: all strings must be valid UTF-8 and array size must be <= 127.
 */
template<typename Allocator = std::allocator<std::string>>
class compact_array_of_strings_tuple_sketch:
  public compact_tuple_sketch<
    array<std::string, Allocator>,
    typename std::allocator_traits<Allocator>::template rebind_alloc<array<std::string, Allocator>>
  > {
public:
  using array_of_strings = array<std::string, Allocator>;
  using summary_allocator = typename std::allocator_traits<Allocator>::template rebind_alloc<array_of_strings>;
  using Base = compact_tuple_sketch<array_of_strings, summary_allocator>;
  using vector_bytes = typename Base::vector_bytes;

  template<typename Sketch>
  compact_array_of_strings_tuple_sketch(const Sketch& sketch, bool ordered = true);

  void serialize(std::ostream& os) const;
  vector_bytes serialize(unsigned header_size_bytes = 0) const;

  static compact_array_of_strings_tuple_sketch deserialize(std::istream& is, uint64_t seed = DEFAULT_SEED,
      const Allocator& allocator = Allocator());
  static compact_array_of_strings_tuple_sketch deserialize(const void* bytes, size_t size, uint64_t seed = DEFAULT_SEED,
      const Allocator& allocator = Allocator());

private:
  explicit compact_array_of_strings_tuple_sketch(Base&& base);
};

/**
 * Extended class of update_tuple_sketch for array of strings
 * Requirements: all strings must be valid UTF-8 and array size must be <= 127.
 */
template<typename Allocator = std::allocator<std::string>>
class update_array_of_strings_tuple_sketch:
  public update_tuple_sketch<
    array<std::string, Allocator>,
    array<std::string, Allocator>,
    default_array_of_strings_update_policy<Allocator>,
    typename std::allocator_traits<Allocator>::template rebind_alloc<array<std::string, Allocator>>
  > {
public:
  using array_of_strings = array<std::string, Allocator>;
  using summary_allocator = typename std::allocator_traits<Allocator>::template rebind_alloc<array_of_strings>;
  using policy_type = default_array_of_strings_update_policy<Allocator>;
  using Base = update_tuple_sketch<
    array_of_strings,
    array_of_strings,
    policy_type,
    summary_allocator
  >;
  using resize_factor = typename Base::resize_factor;
  class builder;
  using Base::update;

  void update(const array_of_strings& key, const array_of_strings& value);
  compact_array_of_strings_tuple_sketch<Allocator> compact(bool ordered = true) const;

private:
  update_array_of_strings_tuple_sketch(uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf, float p, uint64_t theta,
      uint64_t seed, const policy_type& policy, const summary_allocator& allocator);

  // Matches Java Util.PRIME for ArrayOfStrings key hashing.
  static constexpr uint64_t STRING_ARR_HASH_SEED = 0x7A3CCA71ULL;

  static uint64_t hash_key(const array_of_strings& key);
};

template<typename Allocator>
class update_array_of_strings_tuple_sketch<Allocator>::builder:
  public tuple_base_builder<builder, policy_type, summary_allocator> {
public:
  builder(const policy_type& policy = policy_type(), const summary_allocator& allocator = summary_allocator());

  update_array_of_strings_tuple_sketch build() const;
};

} /* namespace datasketches */

#include "array_of_strings_sketch_impl.hpp"

#endif
