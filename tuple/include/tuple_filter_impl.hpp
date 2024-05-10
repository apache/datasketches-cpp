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

#ifndef TUPLE_FILTER_IMPL_HPP_
#define TUPLE_FILTER_IMPL_HPP_

namespace datasketches {

template<typename S, typename A>
tuple_filter<S, A>::tuple_filter(const A& allocator):
allocator_(allocator)
{}

template<typename S, typename A>
template<typename Sketch, typename Predicate>
auto tuple_filter<S, A>::compute(const Sketch& sketch, const Predicate& predicate) const -> CompactSketch {
  std::vector<Entry, AllocEntry> entries(allocator_);
  entries.reserve(sketch.get_num_retained());
  std::copy_if(
    sketch.begin(),
    sketch.end(),
    std::back_inserter(entries),
    [&predicate](const Entry& e) {return predicate(e.second);}
  );
  entries.shrink_to_fit();
  return CompactSketch(
    !sketch.is_estimation_mode() && entries.empty(),
    sketch.is_ordered(),
    sketch.get_seed_hash(),
    sketch.get_theta64(),
    std::move(entries)
  );
}

}

#endif
