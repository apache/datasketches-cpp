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

#ifndef QUADRATICALLY_INTERPOLATED_MAPPING_IMPL_HPP
#define QUADRATICALLY_INTERPOLATED_MAPPING_IMPL_HPP

#include "quadratically_interpolated_mapping.hpp"

namespace datasketches {

inline QuadraticallyInterpolatedMapping::QuadraticallyInterpolatedMapping(const double& relative_accuracy) :
  LogLikeIndexMapping<QuadraticallyInterpolatedMapping>(compute_gamma(require_valid_relative_accuracy(relative_accuracy), CORRECTING_FACTOR), 0.0) {}

inline QuadraticallyInterpolatedMapping::QuadraticallyInterpolatedMapping(const double& gamma, const double& index_offset) :
  LogLikeIndexMapping<QuadraticallyInterpolatedMapping>(gamma, index_offset) {}


inline double QuadraticallyInterpolatedMapping::log(const double& value) const {
  // int64_t value_bits;
  // std::memcpy(&value_bits, &value, sizeof(value));
  //
  // const int64_t mantissa_plus_one_bits = (value_bits & 0x000FFFFFFFFFFFFFL) | 0x3FF0000000000000L;
  // double mantissa_plus_one;
  // std::memcpy(&mantissa_plus_one, &mantissa_plus_one_bits, sizeof(mantissa_plus_one_bits));
  //
  // const double exponent = static_cast<double>(((value_bits & 0x7FF0000000000000L) >> 52) - 1023);

  int exponent = 0;
  const double mantissa = 2 * std::frexp(value, &exponent);

  return exponent - 1 - (mantissa - 5.0) * (mantissa- 1) * ONE_THIRD;
}

inline double QuadraticallyInterpolatedMapping::log_inverse(const double& index) const {
  const int exponent = static_cast<int>(std::floor(index));
  const double mantissa_plus_one = 3.0 - std::sqrt(4.0 - 3.0 * (index - exponent));

  return std::ldexp(mantissa_plus_one, exponent);
}

inline IndexMappingLayout QuadraticallyInterpolatedMapping::layout() const {
  return IndexMappingLayout::LOG_QUADRATIC;
}
}

#endif //QUADRATICALLY_INTERPOLATED_MAPPING_IMPL_HPP
