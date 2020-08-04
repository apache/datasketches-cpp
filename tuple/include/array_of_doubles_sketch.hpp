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

#ifndef ARRAY_OF_DOUBLES_SKETCH_HPP_
#define ARRAY_OF_DOUBLES_SKETCH_HPP_

#include <array>

#include "serde.hpp"
#include "tuple_sketch.hpp"

namespace datasketches {

// equivalent of ArrayOfDoublesSketch in Java

template<int num>
struct array_of_doubles_update_policy {
  std::array<double, num> create() const {
    return std::array<double, num>();
  }
  void update(std::array<double, num>& summary, const std::array<double, num>& update) const {
    for (int i = 0; i < num; ++i) summary[i] += update[i];
  }
};

template<int num, typename A = std::allocator<std::array<double, num>>>
using update_array_of_doubles_sketch = update_tuple_sketch<
    std::array<double, num>, std::array<double, num>, array_of_doubles_update_policy<num>, A>;

template<int num, typename A = std::allocator<std::array<double, num>>>
class compact_array_of_doubles_sketch: public compact_tuple_sketch<std::array<double, num>, A> {
public:
  using Summary = std::array<double, num>;
  using Base = compact_tuple_sketch<Summary, A>;
  using Entry = typename Base::Entry;
  using AllocEntry = typename Base::AllocEntry;
  using AllocU64 = typename Base::AllocU64;

  static const uint8_t SERIAL_VERSION = 1;
  static const uint8_t SKETCH_FAMILY = 9;
  static const uint8_t SKETCH_TYPE = 3;
  enum flags { UNUSED1, UNUSED2, IS_EMPTY, HAS_ENTRIES, IS_ORDERED };

  compact_array_of_doubles_sketch(const Base& other, bool ordered = true);

  void serialize(std::ostream& os) const;

  static compact_array_of_doubles_sketch deserialize(std::istream& is, uint64_t seed = DEFAULT_SEED, const A& allocator = A());

  // for internal use
  compact_array_of_doubles_sketch(bool is_empty, bool is_ordered, uint16_t seed_hash, uint64_t theta, std::vector<Entry, AllocEntry>&& entries);
};

} /* namespace datasketches */

#include "array_of_doubles_sketch_impl.hpp"

#endif
