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

#include <stdexcept>
#include <string>

namespace datasketches {

template<typename Array, typename Policy, typename Allocator>
array_tuple_intersection<Array, Policy, Allocator>::array_tuple_intersection(uint64_t seed, const Policy& policy, const Allocator& allocator):
Base(seed, policy, allocator) {}

template<typename Array, typename Policy, typename Allocator>
template<typename FwdSketch>
void array_tuple_intersection<Array, Policy, Allocator>::update(FwdSketch&& sketch) {
  const uint8_t num_values = this->state_.get_policy().get_external_policy().get_num_values();
  if (sketch.get_num_values() != num_values) {
    throw std::invalid_argument("number of values mismatch: intersection has " + std::to_string(num_values)
        + ", sketch has " + std::to_string(sketch.get_num_values()));
  }
  Base::update(std::forward<FwdSketch>(sketch));
}

template<typename Array, typename Policy, typename Allocator>
auto array_tuple_intersection<Array, Policy, Allocator>::get_result(bool ordered) const -> CompactSketch {
  return CompactSketch(this->state_.get_policy().get_external_policy().get_num_values(), Base::get_result(ordered));
}

} /* namespace datasketches */
