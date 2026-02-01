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

template<typename Allocator>
struct array_of_strings_types {
  using string_allocator = typename std::allocator_traits<Allocator>::template rebind_alloc<char>;
  using string_type = std::basic_string<char, std::char_traits<char>, string_allocator>;
  using array_allocator = typename std::allocator_traits<Allocator>::template rebind_alloc<string_type>;
  using array_of_strings = array<string_type, array_allocator>;
};

// default update policy for an array of strings
template<typename Allocator = std::allocator<char>>
class default_array_of_strings_update_policy {
public:
  using string_allocator = typename array_of_strings_types<Allocator>::string_allocator;
  using string_type = typename array_of_strings_types<Allocator>::string_type;
  using array_allocator = typename array_of_strings_types<Allocator>::array_allocator;
  using array_of_strings = typename array_of_strings_types<Allocator>::array_of_strings;

  explicit default_array_of_strings_update_policy(const Allocator& allocator = Allocator());

  array_of_strings create() const;

  void update(array_of_strings& array, const array_of_strings& input) const;

  void update(array_of_strings& array, const array_of_strings* input) const;

private:
  Allocator allocator_;
};

// serializer/deserializer for an array of strings
// Requirements: all strings must be valid UTF-8 and array size must be <= 127.
template<typename Allocator = std::allocator<char>>
struct default_array_of_strings_serde {
  using string_allocator = typename array_of_strings_types<Allocator>::string_allocator;
  using string_type = typename array_of_strings_types<Allocator>::string_type;
  using array_allocator = typename array_of_strings_types<Allocator>::array_allocator;
  using array_of_strings = typename array_of_strings_types<Allocator>::array_of_strings;
  using summary_allocator = typename std::allocator_traits<Allocator>::template rebind_alloc<array_of_strings>;

  explicit default_array_of_strings_serde(const Allocator& allocator = Allocator());

  void serialize(std::ostream& os, const array_of_strings* items, unsigned num) const;
  void deserialize(std::istream& is, array_of_strings* items, unsigned num) const;
  size_t serialize(void* ptr, size_t capacity, const array_of_strings* items, unsigned num) const;
  size_t deserialize(const void* ptr, size_t capacity, array_of_strings* items, unsigned num) const;
  size_t size_of_item(const array_of_strings& item) const;

private:
  Allocator allocator_;
  summary_allocator summary_allocator_;
  static void check_num_nodes(uint8_t num_nodes);
  static uint32_t compute_total_bytes(const array_of_strings& item);
  static void check_utf8(const string_type& value);
};

/**
 * Hashes an array of strings using ArrayOfStrings-compatible hashing.
 */
template<typename Allocator = std::allocator<char>>
uint64_t hash_array_of_strings_key(const typename array_of_strings_types<Allocator>::array_of_strings& key);

/**
 * Extended class of compact_tuple_sketch for array of strings
 * Requirements: all strings must be valid UTF-8 and array size must be <= 127.
 */
template<typename Allocator = std::allocator<char>>
class compact_array_of_strings_tuple_sketch:
  public compact_tuple_sketch<
    typename array_of_strings_types<Allocator>::array_of_strings,
    typename std::allocator_traits<Allocator>::template rebind_alloc<
      typename array_of_strings_types<Allocator>::array_of_strings
    >
  > {
public:
  using array_of_strings = typename array_of_strings_types<Allocator>::array_of_strings;
  using summary_allocator = typename std::allocator_traits<Allocator>::template rebind_alloc<array_of_strings>;
  using Base = compact_tuple_sketch<array_of_strings, summary_allocator>;
  using vector_bytes = typename Base::vector_bytes;
  using Base::serialize;

  /**
   * Copy constructor.
   * Constructs a compact sketch from another sketch (update or compact)
   * @param other sketch to be constructed from
   * @param ordered if true make the resulting sketch ordered
   */
  template<typename Sketch>
  compact_array_of_strings_tuple_sketch(const Sketch& sketch, bool ordered = true);

  /**
   * This method deserializes a sketch from a given stream.
   * @param is input stream
   * @param seed the seed for the hash function that was used to create the sketch
   * @param sd instance of a SerDe
   * @param allocator instance of an Allocator
   * @return an instance of the sketch
   */
  template<typename SerDe = default_array_of_strings_serde<Allocator>>
  static compact_array_of_strings_tuple_sketch deserialize(std::istream& is, uint64_t seed = DEFAULT_SEED,
      const SerDe& sd = SerDe(), const Allocator& allocator = Allocator());

  /**
   * This method deserializes a sketch from a given array of bytes.
   * @param bytes pointer to the array of bytes
   * @param size the size of the array
   * @param seed the seed for the hash function that was used to create the sketch
   * @param sd instance of a SerDe
   * @param allocator instance of an Allocator
   * @return an instance of the sketch
   */
  template<typename SerDe = default_array_of_strings_serde<Allocator>>
  static compact_array_of_strings_tuple_sketch deserialize(const void* bytes, size_t size, uint64_t seed = DEFAULT_SEED,
      const SerDe& sd = SerDe(), const Allocator& allocator = Allocator());

private:
  explicit compact_array_of_strings_tuple_sketch(Base&& base);
};

/**
 * Convenience alias for update_tuple_sketch for array of strings
 */
template<typename Allocator = std::allocator<char>,
         typename Policy = default_array_of_strings_update_policy<Allocator>>
using update_array_of_strings_tuple_sketch = update_tuple_sketch<
  typename array_of_strings_types<Allocator>::array_of_strings,
  typename array_of_strings_types<Allocator>::array_of_strings,
  Policy,
  typename std::allocator_traits<Allocator>::template rebind_alloc<
    typename array_of_strings_types<Allocator>::array_of_strings
  >
>;

/**
 * Converts an array of strings tuple sketch to a compact sketch (ordered or unordered).
 * @param sketch input sketch
 * @param ordered optional flag to specify if an ordered sketch should be produced
 * @return compact array of strings sketch
 */
template<typename Allocator = std::allocator<char>, typename Policy = default_array_of_strings_update_policy<Allocator>>
compact_array_of_strings_tuple_sketch<Allocator> compact_array_of_strings_sketch(
  const update_array_of_strings_tuple_sketch<Allocator, Policy>& sketch, bool ordered = true);

} /* namespace datasketches */

#include "array_of_strings_sketch_impl.hpp"

#endif
