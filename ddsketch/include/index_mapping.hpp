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

#ifndef INDEX_MAPPING_HPP
#define INDEX_MAPPING_HPP
#include <cstdint>

namespace datasketches {

enum class IndexMappingLayout : uint8_t {
  LOG,
  LOG_LINEAR,
  LOG_QUADRATIC,
  LOG_CUBIC,
  LOG_QUARTIC,
};

std::ostream& operator<<(std::ostream& os, const IndexMappingLayout& obj);

/**
 * @class IndexMapping
 * @brief CRTP base exposing the value/index transform API.
 * @tparam Derived concrete mapping type implementing the operations.
 *
 * Provides a uniform interface to map doubles to integer bin indices and back,
 * with bounds and relative-accuracy queries.
 */
template<class Derived>
class IndexMapping {
public:

  /**
   * @brief Map a value to its integer bin index.
   * @param value input value
   * @return index (may throw if out of range)
   */
  int index(const double& value) const;

  /**
   * @brief Representative value for a bin @p index.
   * @param index bin index
   * @return representative value (inverse mapping)
   */
  double value(int index) const;

  /**
   * @brief Lower bound of values mapped to @p index.
   * @param index bin index
   * @return inclusive lower bound
   */
  double lower_bound(int index) const;

  /**
   * @brief Upper bound of values mapped to @p index.
   * @param index bin index
   * @return exclusive upper bound
   */
  double upper_bound(int index) const;

  /**
  * @brief Target relative accuracy (multiplicative error bound).
  * @return relative accuracy in (0,1)
  */
  double get_relative_accuracy() const;

  /**
   * @brief Smallest trackable value.
   * @return minimum indexable value
   */
  double min_indexable_value() const;

  /**
   * @brief Largest trackable value.
   * @return maximum indexable value
   */
  double max_indexable_value() const;

  /**
  * @brief Serialize this mapping to a stream.
  * @param os output stream
  */
  void serialize(std::ostream& os) const;

  /**
   * @brief Deserialize a concrete {@link Derived} mapping from a stream.
   * @param is input stream
   * @return reconstructed mapping
   */
  static Derived deserialize(std::istream& is);

  ~IndexMapping() = default;

protected:

  /**
   * @brief Downcast to {@link Derived}.
   * @return reference to derived
   */
  Derived& derived();

  /**
   * @brief Const downcast to {@link Derived}.
   * @return const reference to derived
   */
  const Derived& derived() const;
};

}

#include "index_mapping_impl.hpp"
#endif //INDEX_MAPPING_HPP
