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

#ifndef QUADRATICALLY_INTERPOLATED_MAPPING_HPP
#define QUADRATICALLY_INTERPOLATED_MAPPING_HPP
#include "log_like_index_mapping.hpp"

namespace datasketches {
/**
 * @class QuadraticallyInterpolatedMapping
 * A fast {@link IndexMapping} that approximates the memory-optimal one (namely {@link
 * LogarithmicMapping}) by extracting the floor value of the logarithm to the base 2 from the binary
 * representations of floating-point values and quadratically interpolating the logarithm
 * in-between.
 */
class QuadraticallyInterpolatedMapping : public LogLikeIndexMapping<QuadraticallyInterpolatedMapping> {
public:

  /**
   * Constructor.
   *
   * @param relative_accuracy
   */
  explicit QuadraticallyInterpolatedMapping(const double& relative_accuracy);

  /**
   * Overloaded constructor.
   * This is meant to be used when deserializing only
   *
   * @param gamma
   * @param index_offset
   */
  QuadraticallyInterpolatedMapping(const double& gamma, const double& index_offset);

  double log(const double& value) const;

  double log_inverse(const double& index) const;

  IndexMappingLayout layout() const;

  static constexpr double BASE = 2.0;
  static constexpr double CORRECTING_FACTOR = 3.0 / (4.0 * 0.69314718055994530941);

private:
  static constexpr double ONE_THIRD = 1.0 / 3.0;
};
}
#endif //QUADRATICALLY_INTERPOLATED_MAPPING_HPP
#include "quadratically_interpolated_mapping_impl.hpp"
