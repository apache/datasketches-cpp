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

#ifndef QUARTICALLY_INTERPOLATED_MAPPING_HPP
#define QUARTICALLY_INTERPOLATED_MAPPING_HPP
#include "log_like_index_mapping.hpp"

namespace datasketches {

class QuarticallyInterpolatedMapping : public LogLikeIndexMapping<QuarticallyInterpolatedMapping> {
private:
  static constexpr double A = -2.0 / 25.0;
  static constexpr double B = 8.0 / 25.0;
  static constexpr double C = -17.0 / 25.0;
  static constexpr double D = 36.0 / 25.0;

public:
  explicit QuarticallyInterpolatedMapping(const double& relative_accuracy);
  QuarticallyInterpolatedMapping(const double& gamma, const double& index_offset);

  double log(const double& value) const override;
  double log_inverse(const double& index) const override;

  IndexMappingLayout layout() const override;

  static constexpr double BASE = 2.0;
  static constexpr double CORRECTING_FACTOR = 1 / (D * std::numbers::ln2);
};
}

#include "quartically_interpolated_mapping_impl.hpp"

#endif //QUARTICALLY_INTERPOLATED_MAPPING_HPP
