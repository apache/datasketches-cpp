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

template<typename A = std::allocator<std::vector<double>>>
class array_of_doubles_update_policy {
public:
  array_of_doubles_update_policy(uint8_t num_values = 1, const A& allocator = A()):
    allocator(allocator), num_values(num_values) {}
  std::vector<double> create() const {
    return std::vector<double>(num_values, 0, allocator);
  }
  void update(std::vector<double>& summary, const std::vector<double>& update) const {
    for (uint8_t i = 0; i < num_values; ++i) summary[i] += update[i];
  }
  uint8_t get_num_values() const {
    return num_values;
  }

private:
  A allocator;
  uint8_t num_values;
};

// forward declaration
template<typename A> class compact_array_of_doubles_sketch;

template<typename A = std::allocator<std::vector<double>>>
class update_array_of_doubles_sketch: public update_tuple_sketch<std::vector<double>, std::vector<double>,
array_of_doubles_update_policy<A>, A> {
public:
  using Base = update_tuple_sketch<std::vector<double>, std::vector<double>, array_of_doubles_update_policy<A>, A>;
  using resize_factor = typename Base::resize_factor;

  class builder;

  compact_array_of_doubles_sketch<A> compact(bool ordered = true) const;
  uint8_t get_num_values() const;

private:
  // for builder
  update_array_of_doubles_sketch(uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf, uint64_t theta,
      uint64_t seed, const array_of_doubles_update_policy<A>& policy, const A& allocator);
};

template<typename A>
class update_array_of_doubles_sketch<A>::builder: public Base::builder {
public:
  builder(const array_of_doubles_update_policy<A>& policy = array_of_doubles_update_policy<A>(), const A& allocator = A());
  update_array_of_doubles_sketch<A> build() const;
};

template<typename A = std::allocator<std::vector<double>>>
class compact_array_of_doubles_sketch: public compact_tuple_sketch<std::vector<double>, A> {
public:
  using Summary = std::vector<double>;
  using Base = compact_tuple_sketch<Summary, A>;
  using Entry = typename Base::Entry;
  using AllocEntry = typename Base::AllocEntry;
  using AllocU64 = typename Base::AllocU64;

  static const uint8_t SERIAL_VERSION = 1;
  static const uint8_t SKETCH_FAMILY = 9;
  static const uint8_t SKETCH_TYPE = 3;
  enum flags { UNUSED1, UNUSED2, IS_EMPTY, HAS_ENTRIES, IS_ORDERED };

  template<typename Sketch>
  compact_array_of_doubles_sketch(const Sketch& other, bool ordered = true);

  uint8_t get_num_values() const;

  void serialize(std::ostream& os) const;

  static compact_array_of_doubles_sketch deserialize(std::istream& is, uint64_t seed = DEFAULT_SEED, const A& allocator = A());

  // for internal use
  compact_array_of_doubles_sketch(bool is_empty, bool is_ordered, uint16_t seed_hash, uint64_t theta, std::vector<Entry, AllocEntry>&& entries, uint8_t num_values);
private:
  uint8_t num_values;
};

} /* namespace datasketches */

#include "array_of_doubles_sketch_impl.hpp"

#endif
