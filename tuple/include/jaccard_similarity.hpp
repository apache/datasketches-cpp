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

#ifndef JACCARD_SIMILARITY_BASE_HPP_
#define JACCARD_SIMILARITY_BASE_HPP_

#include <memory>
#include <array>

#include <theta_union_experimental.hpp>
#include <theta_intersection_experimental.hpp>
#include <tuple_union.hpp>
#include <tuple_intersection.hpp>
#include <bounds_on_ratios_in_theta_sketched_sets.hpp>
#include <ceiling_power_of_2.hpp>
#include <common_defs.hpp>

namespace datasketches {

template<typename Union, typename Intersection, typename ExtractKey>
class jaccard_similarity_base {
public:
  template<typename SketchA, typename SketchB>
  static std::array<double, 3> jaccard(const SketchA& sketch_a, const SketchB& sketch_b) {
    if (&sketch_a == &sketch_b) return {1, 1, 1};
    if (sketch_a.is_empty() && sketch_b.is_empty()) return {1, 1, 1};
    if (sketch_a.is_empty() || sketch_b.is_empty()) return {0, 0, 0};

    // union
    const unsigned count_a = sketch_a.get_num_retained();
    const unsigned count_b = sketch_b.get_num_retained();
    const unsigned lg_k = std::max(log2(ceiling_power_of_2(count_a + count_b)), theta_constants::MIN_LG_K);
    auto u = typename Union::builder().set_lg_k(lg_k).build();
    u.update(sketch_a);
    u.update(sketch_b);
    auto union_ab = u.get_result(false);

    // identical sets
    if (union_ab.get_num_retained() == sketch_a.get_num_retained() &&
        union_ab.get_num_retained() == sketch_b.get_num_retained() &&
        union_ab.get_theta64() == sketch_a.get_theta64() &&
        union_ab.get_theta64() == sketch_b.get_theta64()) {
      return {1, 1, 1};
    }

    // intersection
    Intersection i;
    i.update(sketch_a);
    i.update(sketch_b);
    i.update(union_ab); // ensures that intersection is a subset of the union
    auto inter_abu = i.get_result(false);

    return {
      bounds_on_ratios_in_theta_sketched_sets<ExtractKey>::lower_bound_for_b_over_a(union_ab, inter_abu),
      bounds_on_ratios_in_theta_sketched_sets<ExtractKey>::estimate_of_b_over_a(union_ab, inter_abu),
      bounds_on_ratios_in_theta_sketched_sets<ExtractKey>::upper_bound_for_b_over_a(union_ab, inter_abu)
    };
  }

};

template<typename Allocator>
using theta_jaccard_similarity_alloc = jaccard_similarity_base<theta_union_experimental<Allocator>, theta_intersection_experimental<Allocator>, trivial_extract_key>;

// alias with default allocator for convenience
using theta_jaccard_similarity = theta_jaccard_similarity_alloc<std::allocator<uint64_t>>;

template<
  typename Summary,
  typename IntersectionPolicy,
  typename UnionPolicy = default_union_policy<Summary>,
  typename Allocator = std::allocator<Summary>>
using tuple_jaccard_similarity = jaccard_similarity_base<tuple_union<Summary, UnionPolicy, Allocator>, tuple_intersection<Summary, IntersectionPolicy, Allocator>, pair_extract_key<uint64_t, Summary>>;

} /* namespace datasketches */

# endif
