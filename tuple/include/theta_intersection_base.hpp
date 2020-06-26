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

#ifndef THETA_INTERSECTION_BASE_HPP_
#define THETA_INTERSECTION_BASE_HPP_

namespace datasketches {

template<
  typename Entry,
  typename ExtractKey,
  typename Policy,
  typename Sketch,
  typename CompactSketch,
  typename Allocator
>
class theta_intersection_base {
public:
  using comparator = compare_by_key<Entry, ExtractKey>;
  theta_intersection_base(uint64_t seed, const Policy& policy);
  ~theta_intersection_base();
  void destroy_objects();

  // TODO: copy and move

  void update(const Sketch& sketch);

  CompactSketch get_result(bool ordered = true) const;

private:
  Policy policy_;
  bool is_valid_;
  bool is_empty_;
  uint8_t lg_size_;
  uint16_t seed_hash_;
  uint32_t num_entries_;
  uint64_t theta_;
  Entry* entries_;
};

} /* namespace datasketches */

#include "theta_intersection_base_impl.hpp"

#endif
