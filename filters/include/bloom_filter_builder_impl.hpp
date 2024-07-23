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

#ifndef _BLOOM_FILTER_BUILDER_IMPL_HPP_
#define _BLOOM_FILTER_BUILDER_IMPL_HPP_

#include <cmath>
#include <memory>
#include <vector>

#include "common_defs.hpp"
#include "xxhash64.h"

namespace datasketches {

template<typename A>
uint16_t bloom_filter_builder_alloc<A>::suggest_num_hashes(const uint64_t num_distinct_items,
                                                           const uint64_t num_filter_bits) {
  // TODO: validate inputs > 0
  return static_cast<uint16_t>(std::ceil(static_cast<double>(num_filter_bits) / num_distinct_items * log(2.0)));
}

template<typename A>
uint16_t bloom_filter_builder_alloc<A>::suggest_num_hashes(const double target_false_positive_prob) {
  return static_cast<uint16_t>(std::ceil(-log(target_false_positive_prob) / log(2.0)));
}

template<typename A>
uint64_t bloom_filter_builder_alloc<A>::suggest_num_filter_bits(const uint64_t max_distinct_items,
                                                                const double target_false_positive_prob) {
  return static_cast<uint64_t>(std::ceil(-static_cast<double>(max_distinct_items) * log(target_false_positive_prob) / (log(2.0) * log(2.0))));
}


template<typename A>
bloom_filter_alloc<A> bloom_filter_builder_alloc<A>::create_by_accuracy(const uint64_t num_distinct_items,
                                                                        const double target_false_positive_prob,
                                                                        const A& allocator) {
  union {
    int64_t long_value;
    double double_value;
  } ldu;
  ldu.double_value = random_utils::next_double(random_utils::rand);
  const uint64_t seed = ldu.long_value;
  return create_by_accuracy(num_distinct_items, target_false_positive_prob, seed, allocator);
}

template<typename A>
bloom_filter_alloc<A> bloom_filter_builder_alloc<A>::create_by_accuracy(const uint64_t num_distinct_items,
                                                                        const double target_false_positive_prob,
                                                                        const uint64_t seed,
                                                                        const A& allocator) {
  const uint64_t num_filter_bits = bloom_filter_builder_alloc<A>::suggest_num_filter_bits(num_distinct_items, target_false_positive_prob);
  const uint16_t num_hashes = bloom_filter_builder_alloc<A>::suggest_num_hashes(target_false_positive_prob);
  return bloom_filter_alloc<A>(num_filter_bits, num_hashes, seed, allocator);
}

template<typename A>
bloom_filter_alloc<A> bloom_filter_builder_alloc<A>::create_by_size(const uint64_t num_bits,
                                                                    const uint16_t num_hashes,
                                                                    const A& allocator) {
  union {
    int64_t long_value;
    double double_value;
  } ldu;
  ldu.double_value = random_utils::next_double(random_utils::rand);
  const uint64_t seed = ldu.long_value;
  return create_by_size(num_bits, num_hashes, seed, allocator);
}

template<typename A>
bloom_filter_alloc<A> bloom_filter_builder_alloc<A>::create_by_size(const uint64_t num_bits,
                                                                    const uint16_t num_hashes,
                                                                    const uint64_t seed,
                                                                    const A& allocator) {
  return bloom_filter_alloc<A>(num_bits, num_hashes, seed, allocator);
}

} // namespace datasketches

#endif // _BLOOM_FILTER_BUILDER_IMPL_HPP_