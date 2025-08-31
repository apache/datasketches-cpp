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

#ifndef LINEARLY_INTERPOLATED_MAPPING_IMPL_HPP
#define LINEARLY_INTERPOLATED_MAPPING_IMPL_HPP
#include "linearly_interpolated_mapping.hpp"
#include <cmath>
#include "fast_log2.hpp"

namespace datasketches {

inline LinearlyInterpolatedMapping::LinearlyInterpolatedMapping(const double& relative_accuracy):
  LogLikeIndexMapping<LinearlyInterpolatedMapping>(compute_gamma(require_valid_relative_accuracy(relative_accuracy), CORRECTING_FACTOR), index_offset_shift(relative_accuracy)) {}

inline LinearlyInterpolatedMapping::LinearlyInterpolatedMapping(const double& gamma, const double& index_offset):
  LogLikeIndexMapping<LinearlyInterpolatedMapping>(gamma, index_offset) {}

inline double LinearlyInterpolatedMapping::log(const double& value) const {
  return fast_log2(value);
}

inline double LinearlyInterpolatedMapping::log_inverse(const double& index) const {
  return fast_log2_inverse(index);
}

inline IndexMappingLayout LinearlyInterpolatedMapping::layout() const {
  return IndexMappingLayout::LOG_LINEAR;
}

inline double LinearlyInterpolatedMapping::index_offset_shift(const double& relative_accuracy) {
  return 1 / (std::log1p(2 * relative_accuracy / (1 - relative_accuracy)));
}
}

#endif //LINEARLY_INTERPOLATED_MAPPING_IMPL_HPP
