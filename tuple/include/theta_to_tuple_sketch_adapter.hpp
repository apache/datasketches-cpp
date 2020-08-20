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

#ifndef THETA_TO_TUPLE_SKETCH_ADAPTER_HPP_
#define THETA_TO_TUPLE_SKETCH_ADAPTER_HPP_

#include <memory>

#include "theta_sketch_experimental.hpp"

namespace datasketches {

template<typename Summary, typename Allocator = std::allocator<uint64_t>>
class theta_to_tuple_sketch_adapter {
public:
  theta_to_tuple_sketch_adapter(const update_theta_sketch_experimental<Allocator>& sketch, const Summary& summary);
  theta_to_tuple_sketch_adapter(const compact_theta_sketch_experimental<Allocator>& sketch, const Summary& summary);
  bool is_empty() const;
  bool is_ordered() const;
  uint16_t get_seed_hash() const;
  uint64_t get_theta64() const;

  class const_iterator;
  const_iterator begin();
  const_iterator end();

private:
  const theta_sketch_experimental<Allocator>* sketch_ptr;
  Summary summary;
};

template<typename Summary, typename Allocator>
class theta_to_tuple_sketch_adapter<Summary, Allocator>::const_iterator {
public:
  using Entry = std::pair<uint64_t, Summary>;
  using theta_const_iterator = typename theta_sketch_experimental<Allocator>::const_iterator;

  using iterator_category = std::forward_iterator_tag;
  using value_type = Entry;
  using difference_type = std::ptrdiff_t;
  using pointer = Entry*;
  using reference = Entry&;

  const_iterator(const theta_const_iterator& it, const Summary& summary);
  const_iterator& operator++();
  const_iterator operator++(int);
  bool operator==(const const_iterator& other) const;
  bool operator!=(const const_iterator& other) const;
  Entry& operator*() const;

private:
  theta_const_iterator it;
  Summary summary;
  mutable Entry entry;
};

} /* namespace datasketches */

#include "theta_to_tuple_sketch_adapter_impl.hpp"

#endif
