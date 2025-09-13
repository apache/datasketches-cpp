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

#ifndef _DDSKETCH_HPP_
#define _DDSKETCH_HPP_

#include <utility>
#include <type_traits>
#include <variant>
#include "store.hpp"
#include "store_factory.hpp"
#include "common_defs.hpp"
#include "memory_operations.hpp"

namespace datasketches {

template<store_concept Store, class Mapping>
class DDSketch {
public:
  // TODO
  // DDSketch() = default;
  // template<auto... Args>
  // explicit DDSketch(const double& relative_accuracy):
  // positive_store(store_factory<Store>::new_store(Args...)),
  // negative_store(store_factory<Store>::new_store(Args...)),
  // index_mapping(Mapping(relative_accuracy)) {}

  DDSketch(const Store& positive_store, const Store& negative_store, const Mapping& mapping, const double& zero_count = 0.0, const double& min_indexed_value = 0.0);

  explicit DDSketch(const Mapping& index_mapping);

  explicit DDSketch(double relative_accuracy);

  void update(const double&value, const double& count = 1.0);

  template<store_concept OtherStore>
  void merge(const DDSketch<OtherStore, Mapping>& other);

  double get_rank(const double& item) const;
  double get_quantile(const double& rank) const;

  bool is_empty() const;
  void clear();

  double get_count() const;
  double get_sum() const;
  double get_min() const;
  double get_max() const;

  void serialize(std::ostream& os) const;
  static DDSketch deserialize(std::istream& is);

protected:
  Store positive_store;
  Store negative_store;
  Mapping index_mapping;

  double zero_count;
  const double min_indexed_value;
  const double max_indexed_value;

  // Serialization constants
  static const uint8_t SERIAL_VERSION = 1;
  static const uint8_t FAMILY = 18; // DDSketch family ID
  static const size_t PREAMBLE_SIZE_BYTES = 8;

  void check_value_trackable(const double& value) const;
  template<store_concept OtherStore>
  void check_mergeability(const DDSketch<OtherStore, Mapping>& other) const;
  double get_quantile(const double& rank, const double& count) const;
};

// CTA (class template argument deduction) deduction guides (so you can write `ddsketch sketch(s1);`)
template<store_concept Store, class Mapping>
DDSketch(Store, Mapping) -> DDSketch<Store, Mapping>;

} /* namespace datasketches */

#include "ddsketch_impl.hpp"

#endif // _DDSKETCH_HPP_
