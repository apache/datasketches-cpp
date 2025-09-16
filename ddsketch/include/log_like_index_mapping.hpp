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
#ifndef LOG_LIKE_INDEX_MAPPING_HPP
#define LOG_LIKE_INDEX_MAPPING_HPP
#include "index_mapping.hpp"
#include <cmath>
#include <memory>
#include <stdexcept>

namespace datasketches {

/**
 * @class LogLikeIndexMapping
 * A base class for mappings that are derived from a function that approximates the logarithm.
 *
 * <p>That function is scaled depending on the targeted relative accuracy, the base of the logarithm
 * that log approximates and how well it geometrically pulls apart values from one another,
 * that is to say, the infimum of |(l∘exp)(x)-(l∘exp)(y)|/|x-y| where x ≠ y and l = log
 */
template<class Derived>
class LogLikeIndexMapping : public IndexMapping<Derived> {
public:
  /**
   * Constructor.
   *
   * @param gamma
   * @param index_offset
   */
  LogLikeIndexMapping(const double& gamma, const double& index_offset);

  /**
   * @brief Map a value to its integer bin index.
   *
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
   * @brief Target relative accuracy (multiplicative error bound).
   * @return relative accuracy in (0,1)
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

  template<class A = std::allocator<char>>
  string<A> to_string() const;

  bool operator==(const LogLikeIndexMapping<Derived>& other) const;

private:
  static double compute_relative_accuracy(const double& gamma, const double& correcting_factor);
  static double require_valid_gamma(const double& gamma);
  IndexMappingLayout layout() const;

protected:
  static double require_valid_relative_accuracy(const double& relative_accuracy);
  static double compute_gamma(const double& relative_accuracy, const double& correcting_factor);
  double log(const double& value) const;
  double log_inverse(const double& value) const;

  const double gamma;
  const double index_offset;

  const double relative_accuracy;
  const double multiplier;

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

#include "log_like_index_mapping_impl.hpp"

#endif //LOG_LIKE_INDEX_MAPPING_HPP
