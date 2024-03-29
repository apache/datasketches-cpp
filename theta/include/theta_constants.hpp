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

#ifndef THETA_CONSTANTS_HPP_
#define THETA_CONSTANTS_HPP_

#include <climits>
#include "common_defs.hpp"

namespace datasketches {

/// Theta constants
namespace theta_constants {
  /// hash table resize factor
  using resize_factor = datasketches::resize_factor;
  /// default resize factor
  const resize_factor DEFAULT_RESIZE_FACTOR = resize_factor::X8;

  /// max theta - signed max for compatibility with Java
  const uint64_t MAX_THETA = LLONG_MAX;
  /// min log2 of K
  const uint8_t MIN_LG_K = 5;
  /// max log2 of K
  const uint8_t MAX_LG_K = 26;
  /// default log2 of K
  const uint8_t DEFAULT_LG_K = 12;
}

} /* namespace datasketches */

#endif
