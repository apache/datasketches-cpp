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

#ifndef COUNT_MIN_V2_CASES_HPP_
#define COUNT_MIN_V2_CASES_HPP_

// The cross-language test files of the Count-Min format specification in datasketches-tck,
// section 8: count_min_v2_<case>_<language>.sk, with numHashes 3 and numBuckets 1000.

#include <cstdint>
#include <string>

#include "count_min.hpp"

namespace datasketches {

struct count_min_v2_case {
  const char* name;
  uint64_t seed;
  int64_t n;
};

static const count_min_v2_case count_min_v2_cases[] = {
  {"n0", 9001, 0},
  {"n1", 9001, 1},
  {"n10", 9001, 10},
  {"n100", 9001, 100},
  {"n1000", 9001, 1000},
  {"n10000", 9001, 10000},
  {"seed42_n1000", 42, 1000}
};

// for each i from 0 to n - 1: the 64-bit integer i with weight i + 1, and the decimal string of i with weight 1
inline count_min_sketch<int64_t> make_count_min_v2_case(const count_min_v2_case& c) {
  count_min_sketch<int64_t> sketch(3, 1000, c.seed);
  for (int64_t i = 0; i < c.n; ++i) {
    sketch.update(i, i + 1);
    sketch.update(std::to_string(i), 1);
  }
  return sketch;
}

inline std::string count_min_v2_file_name(const count_min_v2_case& c, const std::string& language) {
  return "count_min_v2_" + std::string(c.name) + "_" + language + ".sk";
}

} /* namespace datasketches */

#endif
