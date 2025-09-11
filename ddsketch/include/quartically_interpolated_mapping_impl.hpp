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

#ifndef QUARTICALLY_INTERPOLATED_MAPPING_IMPL_HPP
#define QUARTICALLY_INTERPOLATED_MAPPING_IMPL_HPP
#include "quartically_interpolated_mapping.hpp"

namespace datasketches {
inline QuarticallyInterpolatedMapping::QuarticallyInterpolatedMapping(const double& relative_accuracy) :
  LogLikeIndexMapping<datasketches::QuarticallyInterpolatedMapping>(compute_gamma(require_valid_relative_accuracy(relative_accuracy), CORRECTING_FACTOR), 0.0) {}

inline QuarticallyInterpolatedMapping::QuarticallyInterpolatedMapping(const double& gamma, const double& index_offset) :
  LogLikeIndexMapping<QuarticallyInterpolatedMapping>(gamma, index_offset) {}

inline double QuarticallyInterpolatedMapping::log(const double &value) const {
  int exponent = 0;
  const double mantissa = 2 * std::frexp(value, &exponent) - 1;

  return (((A * mantissa + B) * mantissa + C) * mantissa + D) * mantissa + exponent - 1;
}

inline double QuarticallyInterpolatedMapping::log_inverse(const double &index) const {
  const auto exponent = static_cast<int>(std::floor(index));
  const double e = static_cast<double>(exponent) - index;

  // Derived from Ferrari's method
  const double alpha = -(3 * B * B) / (8 * A * A) + C / A; // 2.5
  const double beta = (B * B * B) / (8 * A * A * A) - (B * C) / (2 * A * A) + D / A; // -9.0
  const double gamma = -(3 * B * B * B * B) / (256 * A * A * A * A) + (C * B * B) / (16 * A * A * A) - (B * D) / (4 * A * A) + e / A;
  const double p = -(alpha * alpha) / 12 - gamma;
  const double q = -(alpha * alpha * alpha) / 108 + (alpha * gamma) / 3 - (beta * beta) / 8;
  const double r = -q / 2 + std::sqrt((q * q) / 4 + (p * p * p) / 27);
  const double u = std::cbrt(r);
  const double y = -(5 * alpha) / 6 + u - p / (3 * u);
  const double w = std::sqrt(alpha + 2 * y);
  const double x = (-B / (4 * A) + (w - std::sqrt(-(3 * alpha + 2 * y + (2 * beta) / w))) / 2) + 1;

  int tmp = 0;
  double m = std::frexp(x, &tmp);
  double sig = 2.0 * m;
  return std::ldexp(sig, exponent);
}

inline IndexMappingLayout QuarticallyInterpolatedMapping::layout() const {
  return IndexMappingLayout::LOG_QUARTIC;
}



}


#endif //QUARTICALLY_INTERPOLATED_MAPPING_IMPL_HPP
