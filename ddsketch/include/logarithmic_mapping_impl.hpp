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

#ifndef LOGARITHMIC_MAPPING_IMPL_HPP
#define LOGARITHMIC_MAPPING_IMPL_HPP
#include "logarithmic_mapping.hpp"

namespace datasketches {

inline LogarithmicMapping::LogarithmicMapping(const double& relative_accuracy) :
  LogLikeIndexMapping<LogarithmicMapping>(compute_gamma(require_valid_relative_accuracy(relative_accuracy), 1.0), 0.0) {}

inline LogarithmicMapping::LogarithmicMapping(const double& gamma, const double& index_offset) :
  LogLikeIndexMapping<LogarithmicMapping>(gamma, index_offset) {}

inline double LogarithmicMapping::log(const double& value) const {
  return std::log(value);
}

inline double LogarithmicMapping::log_inverse(const double &index) const {
  return std::exp(index);
}

inline IndexMappingLayout LogarithmicMapping::layout() const {
  return IndexMappingLayout::LOG;
}
}


#endif //LOGARITHMIC_MAPPING_IMPL_HPP
