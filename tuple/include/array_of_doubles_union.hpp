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

#ifndef ARRAY_OF_DOUBLES_UNION_HPP_
#define ARRAY_OF_DOUBLES_UNION_HPP_

#include <vector>
#include <memory>

#include "tuple_union.hpp"

namespace datasketches {

struct array_of_doubles_union_policy {
  void operator()(std::vector<double>& summary, const std::vector<double>& other) const {
    for (size_t i = 0; i < summary.size(); ++i) {
      summary[i] += other[i];
    }
  }
};

template<typename Allocator = std::allocator<std::vector<double>>>
using array_of_doubles_union_alloc = tuple_union<std::vector<double>, array_of_doubles_union_policy, Allocator>;

// alias with default allocator
using array_of_doubles_union = array_of_doubles_union_alloc<>;

} /* namespace datasketches */

#endif
