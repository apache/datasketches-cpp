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

#include <set>

#include "quadratically_interpolated_mapping.hpp"

namespace datasketches {

inline QuadraticallyInterpolatedMapping::QuadraticallyInterpolatedMapping(const double& relative_accuracy) :
  LogLikeIndexMapping<QuadraticallyInterpolatedMapping>(compute_gamma(require_valid_relative_accuracy(relative_accuracy), CORRECTING_FACTOR), 0.0) {}

inline QuadraticallyInterpolatedMapping::QuadraticallyInterpolatedMapping(const double& gamma, const double& index_offset) :
  LogLikeIndexMapping<QuadraticallyInterpolatedMapping>(gamma, index_offset) {}


inline double QuadraticallyInterpolatedMapping::log(const double& value) const {
  int64_t value_bits;
  std::memcpy(&value_bits, &value, sizeof(value));

  const int64_t mantissa_plus_one_bits = (value_bits & 0x000FFFFFFFFFFFFFL) | 0x3FF0000000000000L;
  double mantissa_plus_one;
  std::memcpy(&mantissa_plus_one, &mantissa_plus_one_bits, sizeof(mantissa_plus_one_bits));

  const double exponent = static_cast<double>(((value_bits & 0x7FF0000000000000L) >> 52) - 1023);

  return exponent - (mantissa_plus_one - 5.0) * (mantissa_plus_one - 1) * ONE_THIRD;
}

inline double QuadraticallyInterpolatedMapping::log_inverse(const double& index) const {
  const int64_t exponent = static_cast<int64_t>(std::floor(index));
  const double mantissa_plus_one = 3.0 - std::sqrt(4.0 - 3.0 * (index - exponent));

  int64_t result_bits = (static_cast<uint64_t>(exponent + 1023) << 52) & 0x7FF0000000000000L;

  uint64_t mantissa_plus_one_bits;
  std::memcpy(&mantissa_plus_one_bits, &mantissa_plus_one, sizeof(mantissa_plus_one));

  result_bits |= (mantissa_plus_one_bits & 0x000FFFFFFFFFFFFFL);

  double result;
  std::memcpy(&result, &result_bits, sizeof(result_bits));
  return result;
}

inline IndexMappingLayout QuadraticallyInterpolatedMapping::layout() const {
  return IndexMappingLayout::LOG_QUADRATIC;
}
}

#endif //QUADRATICALLY_INTERPOLATED_MAPPING_IMPL_HPP
