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

#ifndef INV_POW2_HPP_
#define INV_POW2_HPP_

#include <cstdint>
#include <cstring>

namespace datasketches {

/**
 * Computes 2^-exp exactly by building the double from its exponent bits, as Java's Util.invPow2 does.
 * Exact for exp from 0 to 1022.
 * @param exp the exponent, from 0 to 1022
 * @return 2^-exp
 */
static inline double inv_pow2(uint32_t exp) {
  const uint64_t bits = static_cast<uint64_t>(1023 - exp) << 52;
  double value;
  std::memcpy(&value, &bits, sizeof(value));
  return value;
}

} /* namespace datasketches */

#endif
