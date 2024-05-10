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

#ifndef TUPLE_FILTER_HPP_
#define TUPLE_FILTER_HPP_

#include <tuple_sketch.hpp>

namespace datasketches {

template<typename Summary, typename Allocator = std::allocator<Summary>>
class tuple_filter {
public:
  using Entry = std::pair<uint64_t, Summary>;
  using CompactSketch = compact_tuple_sketch<Summary, Allocator>;
  using AllocEntry = typename std::allocator_traits<Allocator>::template rebind_alloc<Entry>;

  explicit tuple_filter(const Allocator& allocator = Allocator());

  template<typename Sketch, typename Predicate>
  CompactSketch compute(const Sketch& sketch, const Predicate& predicate) const;

private:
  Allocator allocator_;
};

}

#include "tuple_filter_impl.hpp"

#endif
