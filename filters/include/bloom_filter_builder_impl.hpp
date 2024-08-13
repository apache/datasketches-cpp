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
uint64_t bloom_filter_builder_alloc<A>::generate_random_seed() {
  union {
    uint64_t long_value;
    double double_value;
  } ldu;
  ldu.double_value = random_utils::next_double(random_utils::rand);
  return ldu.long_value;
}

template<typename A>
uint16_t bloom_filter_builder_alloc<A>::suggest_num_hashes(const uint64_t max_distinct_items,
                                                           const uint64_t num_filter_bits) {
  if (max_distinct_items == 0) {
    throw std::invalid_argument("maximum number of distinct items must be strictly positive");
  }
  if (num_filter_bits == 0) {
    throw std::invalid_argument("number of bits in the filter must be strictly positive");
  } else if (num_filter_bits > bloom_filter_alloc<A>::MAX_FILTER_SIZE_BITS) {
    throw std::invalid_argument("number of bits in the filter must be less than 2^63");
  }
  return static_cast<uint16_t>(std::ceil(static_cast<double>(num_filter_bits) / max_distinct_items * log(2.0)));
}

template<typename A>
uint16_t bloom_filter_builder_alloc<A>::suggest_num_hashes(const double target_false_positive_prob) {
  validate_accuracy_inputs(100, target_false_positive_prob); // max_distinct_items is an arbitrary valid value
  return static_cast<uint16_t>(std::ceil(-log(target_false_positive_prob) / log(2.0)));
}

template<typename A>
uint64_t bloom_filter_builder_alloc<A>::suggest_num_filter_bits(const uint64_t max_distinct_items,
                                                                const double target_false_positive_prob) {
  validate_accuracy_inputs(max_distinct_items, target_false_positive_prob);
  return static_cast<uint64_t>(std::ceil(-static_cast<double>(max_distinct_items) * log(target_false_positive_prob) / (log(2.0) * log(2.0))));
}

template<typename A>
bloom_filter_alloc<A> bloom_filter_builder_alloc<A>::create_by_accuracy(const uint64_t max_distinct_items,
                                                                        const double target_false_positive_prob,
                                                                        const uint64_t seed,
                                                                        const A& allocator) {
  validate_accuracy_inputs(max_distinct_items, target_false_positive_prob);
  const uint64_t num_filter_bits = bloom_filter_builder_alloc<A>::suggest_num_filter_bits(max_distinct_items, target_false_positive_prob);
  const uint16_t num_hashes = bloom_filter_builder_alloc<A>::suggest_num_hashes(target_false_positive_prob);
  return bloom_filter_alloc<A>(num_filter_bits, num_hashes, seed, allocator);
}

template<typename A>
bloom_filter_alloc<A> bloom_filter_builder_alloc<A>::create_by_size(const uint64_t num_bits,
                                                                    const uint16_t num_hashes,
                                                                    const uint64_t seed,
                                                                    const A& allocator) {
  validate_size_inputs(num_bits, num_hashes);
  return bloom_filter_alloc<A>(num_bits, num_hashes, seed, allocator);
}

template<typename A>
bloom_filter_alloc<A> bloom_filter_builder_alloc<A>::initialize_by_accuracy(void* memory,
                                                                            const size_t length_bytes,
                                                                            const uint64_t max_distinct_items,
                                                                            const double target_false_positive_prob,
                                                                            const uint64_t seed,
                                                                            const A& allocator) {
  validate_accuracy_inputs(max_distinct_items, target_false_positive_prob);
  const uint64_t num_filter_bits = bloom_filter_builder_alloc<A>::suggest_num_filter_bits(max_distinct_items, target_false_positive_prob);
  const uint16_t num_hashes = bloom_filter_builder_alloc<A>::suggest_num_hashes(target_false_positive_prob);
  return bloom_filter_alloc<A>(static_cast<uint8_t*>(memory), length_bytes, num_filter_bits, num_hashes, seed, allocator);
}

template<typename A>
bloom_filter_alloc<A> bloom_filter_builder_alloc<A>::initialize_by_size(void* memory,
                                                                        const size_t length_bytes,
                                                                        const uint64_t num_bits,
                                                                        const uint16_t num_hashes,
                                                                        const uint64_t seed,
                                                                        const A& allocator) {
  validate_size_inputs(num_bits, num_hashes);
  return bloom_filter_alloc<A>(static_cast<uint8_t*>(memory), length_bytes, num_bits, num_hashes, seed, allocator);
}

template<typename A>
void bloom_filter_builder_alloc<A>::validate_size_inputs(uint64_t num_bits, uint16_t num_hashes) {
  if (num_bits == 0) {
    throw std::invalid_argument("number of bits in the filter must be strictly positive");
  } else if (num_bits > bloom_filter_alloc<A>::MAX_FILTER_SIZE_BITS) {
    throw std::invalid_argument("number of bits in the filter must be less than 2^63");
  }
  if (num_hashes == 0) {
    throw std::invalid_argument("number of hashes for the filter must be strictly positive");
  }
}

template<typename A>
void bloom_filter_builder_alloc<A>::validate_accuracy_inputs(uint64_t max_distinct_items, double target_false_positive_prob) {
  if (max_distinct_items == 0) {
    throw std::invalid_argument("maximum number of distinct items must be strictly positive");
  }
  if (target_false_positive_prob <= 0.0 || target_false_positive_prob > 1.0) {
    throw std::invalid_argument("target false positive probability must be a valid probability strictly greater than 0.0");
  }
}

} // namespace datasketches

#endif // _BLOOM_FILTER_BUILDER_IMPL_HPP_