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

#ifndef DENSE_STORE_HPP
#define DENSE_STORE_HPP

#include <cstdint>
#include <limits>

#include "store.hpp"
#include <vector>

namespace datasketches {
template<typename Allocator>
class DenseStore : public Store<Allocator> {
public:
  using bins_type = std::vector<uint64_t, typename std::allocator_traits<Allocator>::template rebind_alloc<uint64_t>>;
  using size_type = typename bins_type::size_type;

  DenseStore();
  explicit DenseStore(const int& array_length_growth_increment);
  explicit DenseStore(const int& array_length_growth_increment, const int& array_length_overhead);
  explicit DenseStore(const DenseStore& other);


  void add(int index) override;
  void add(int index, uint64_t count) override;
  void add(const Bin& bin) override;
  DenseStore<Allocator>* copy() const override = 0;
  void clear() override;
  bool is_empty() const override;
  int get_max_index() const override;
  int get_min_index() const override;
  uint64_t get_total_count() const override;
  void merge(const Store<Allocator>& other) override = 0;

  ~DenseStore() override = default;

private:
  bins_type bins;
  size_type offset;
  size_type min_index;
  size_type max_index;

  const int array_length_growth_increment;
  const int array_length_overhead;

  static constexpr int DEFAULT_ARRAY_LENGTH_GROWTH_INCREMENT = 64;
  static constexpr double DEFAULT_ARRAY_LENGTH_OVERHEAD_RATIO = 0.1;

  uint64_t get_total_count(size_type from_index, size_type to_index) const;
  virtual size_type normalize(size_type index) = 0;
  virtual size_type adjust(size_type newMinIndex, size_type newMaxIndex) = 0;
  void extend_range(size_type index);
  void extend_range(size_type new_min_ndex, size_type new_max_index);
  void shift_bins(size_type shift); // private
  void center_bins(size_type new_min_index, size_type new_max_index); // private
  virtual size_type get_new_length(size_type new_min_index, size_type new_max_index) const; // private
  void reset_bins(); // private
  void reset_bins(size_type from_index, size_type to_index); // private
};
}

#endif //DENSE_STORE_HPP
