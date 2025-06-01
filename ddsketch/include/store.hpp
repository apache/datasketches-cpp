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

#ifndef STORE_HPP
#define STORE_HPP

#include "bin.hpp"

namespace datasketches {
template<typename Allocator>
class Store {
public:
  using allocator_type = Allocator;

  virtual ~Store() = default;
  virtual void add(int index) = 0;
  virtual void add(int index, uint64_t count) = 0;
  virtual void add(const Bin& bin) = 0;
  virtual Store* copy() const= 0;
  virtual void clear() = 0;
  virtual bool is_empty() const = 0;
  virtual int get_max_index() const = 0 ;
  virtual int get_min_index() const = 0;
  virtual uint64_t get_total_count() const = 0;
  virtual void merge(const Store& other) = 0;
};
}
#endif //STORE_HPP
