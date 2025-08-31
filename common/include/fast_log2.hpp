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

#ifndef FAST_LOG2_HPP
#define FAST_LOG2_HPP

#include <iostream>
#include <cstring>
#include <cmath>

namespace datasketches {

static inline double fast_log2(const double num) {
  int64_t num_bits;
  std::memcpy(&num_bits, &num, sizeof(num));

  const int64_t exponent = ((num_bits & 0x7FF0000000000000L) >> 52) - 1023;
  const int64_t mantissa_plus_one_bits = (num_bits & 0x000FFFFFFFFFFFFFL) | 0x3FF0000000000000L;

  double mantissa_plus_one;
  std::memcpy(&mantissa_plus_one, &mantissa_plus_one_bits, sizeof(mantissa_plus_one_bits));

  return static_cast<double>(exponent) + mantissa_plus_one - 1.0;
}

static inline double fast_log2_inverse(const double num) {
  const int64_t exponent = static_cast<int64_t>(std::floor(num));
  const double mantissa_plus_one = num - exponent + 1.0;

  int64_t result_bits = (static_cast<uint64_t>(exponent + 1023) << 52) & 0x7FF0000000000000L;

  uint64_t mantissa_plus_one_bits;
  std::memcpy(&mantissa_plus_one_bits, &mantissa_plus_one, sizeof(mantissa_plus_one));

  result_bits |= (mantissa_plus_one_bits & 0x000FFFFFFFFFFFFFL);

  double result;
  std::memcpy(&result, &result_bits, sizeof(result_bits));
  return result;
}
}

#endif //FAST_LOG2_HPP
