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

#ifndef LINEARLY_INTERPOLATED_MAPPING_HPP
#define LINEARLY_INTERPOLATED_MAPPING_HPP

#include "index_mapping.hpp"
#include "log_like_index_mapping.hpp"
#include <numbers>


namespace datasketches {

class LinearlyInterpolatedMapping : public LogLikeIndexMapping<LinearlyInterpolatedMapping> {
public:
  explicit LinearlyInterpolatedMapping(const double& relative_accuracy);
  LinearlyInterpolatedMapping(const double& gamma, const double& index_offset);

  double log(const double& value) const override;
  double log_inverse(const double& index) const override;

  IndexMappingLayout layout() const override;

  static constexpr double BASE = 2.0;
  static constexpr double CORRECTING_FACTOR = std::numbers::log2e;

private:
  static double index_offset_shift(const double& relative_accuracy);
};
}

#include "linearly_interpolated_mapping_impl.hpp"

#endif //LINEARLY_INTERPOLATED_MAPPING_HPP
