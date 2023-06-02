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

#ifndef THETA_UNION_HPP_
#define THETA_UNION_HPP_

#include "serde.hpp"
#include "theta_sketch.hpp"
#include "theta_union_base.hpp"

namespace datasketches {

template<typename Allocator = std::allocator<uint64_t>,
        template<typename, typename, typename> class Table = theta_update_sketch_base>
class theta_union_alloc {
public:
  using Entry = uint64_t;
  using ExtractKey = trivial_extract_key;
  using Sketch = theta_sketch_alloc<Allocator>;
  using CompactSketch = compact_theta_sketch_alloc<Allocator>;
  using resize_factor = theta_constants::resize_factor;
  using theta_table = Table<Entry, ExtractKey, Allocator>;


  struct nop_policy {
    void operator()(uint64_t internal_entry, uint64_t incoming_entry) const {
      unused(internal_entry);
      unused(incoming_entry);
    }
  };
  using State = theta_union_base<Entry, ExtractKey, nop_policy, Sketch, CompactSketch, Allocator, Table>;

  // No constructor here. Use builder instead.
  class builder;

  /**
   * This method is to update the union with a given sketch
   * @param sketch to update the union with
   */
  template<typename FwdSketch>
  void update(FwdSketch&& sketch);

  /**
   * This method produces a copy of the current state of the union as a compact sketch.
   * @param ordered optional flag to specify if ordered sketch should be produced
   * @return the result of the union
   */
  CompactSketch get_result(bool ordered = true) const;

  /**
   * Reset the union to the initial empty state
   */
  void reset();

private:
  State state_;

  // for builder
  theta_union_alloc(theta_table&& table);
};

template<typename A, template<typename, typename, typename> class T>
class theta_union_alloc<A, T>::builder: public theta_base_builder<builder, A> {
public:
  using Table = theta_union_alloc::theta_table;

  builder(const A& allocator = A());

  /**
   * This is to create an instance of the union with predefined parameters.
   * @return an instance of the union
   */
  theta_union_alloc<A, T> build() const;
};

// alias with default allocator for convenience
using theta_union = theta_union_alloc<std::allocator<uint64_t>>;

} /* namespace datasketches */

#include "theta_union_impl.hpp"

#endif
