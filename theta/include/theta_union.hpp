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

#include <memory>
#include <functional>
#include <climits>

#include "theta_sketch.hpp"

namespace datasketches {

/*
 * author Alexander Saydakov
 * author Lee Rhodes
 * author Kevin Lang
 */

template<typename A>
class theta_union_alloc {
public:
  class builder;

  // No constructor here. Use builder instead.

  /**
   * This method is to update the union with a given sketch
   * @param sketch to update the union with
   */
  void update(const theta_sketch_alloc<A>& sketch);

  /**
   * This method is to update the union with a given string.
   * @param value string to update the union with
   */
  void update(const std::string &value);
  
  /**
   * This method is to update the union with a given unsigned 64-bit integer.
   * @param value uint64_t to update the union with
   */
  void update(uint64_t value);
  
  /**
   * This method is to update the union with a given signed 64-bit integer.
   * @param value int64_t to update the union with
   */
  void update(int64_t value);
  
  /**
   * This method is to update the union with a given unsigned 32-bit integer.
   * For compatibility with Java implementation.
   * @param value uint32_t to update the union with
   */
  void update(uint32_t value);
  
  /**
   * This method is to update the union with a given signed 32-bit integer.
   * For compatibility with Java implementation.
   * @param value int32_t to update the union with
   */
  void update(int32_t value);
  
  /**
   * This method is to update the union with a given unsigned 16-bit integer.
   * For compatibility with Java implementation.
   * @param value uint16_t to update the union with
   */
  void update(uint16_t value);
  
  /**
   * This method is to update the union with a given signed 16-bit integer.
   * For compatibility with Java implementation.
   * @param value int16_t to update the union with
   */
  void update(int16_t value);
  
  /**
   * This method is to update the union with a given unsigned 8-bit integer.
   * For compatibility with Java implementation.
   * @param value uint8_t to update the union with
   */
  void update(uint8_t value);
  
  /**
   * This method is to update the union with a given signed 8-bit integer.
   * For compatibility with Java implementation.
   * @param value int8_t to update the union with
   */
  void update(int8_t value);
  
  /**
   * This method is to update the union with a given double-precision floating point value.
   * For compatibility with Java implementation.
   * @param value double to update the union with
   */
  void update(double value);
  
  /**
   * This method is to update the union with a given floating point value.
   * For compatibility with Java implementation.
   * @param value float to update the union with
   */
  void update(float value);
  
  /**
   * This method is to update the union with given data of any type.
   * This is a "universal" update that covers all cases above,
   * but may produce different hashes.
   * Be very careful to hash input values consistently using the same approach
   * both over time and on different platforms
   * and while passing sketches between C++ environment and Java environment.
   * Otherwise two sketches that should represent overlapping sets will be disjoint
   * For instance, for signed 32-bit values call update(int32_t) method above,
   * which does widening conversion to int64_t, if compatibility with Java is expected
   * @param data pointer to the data
   * @param length of the data in bytes
   */
  void update(const void *data, unsigned length);

  /**
   * This method produces a copy of the current state of the union as a compact sketch.
   * @param ordered optional flag to specify if ordered sketch should be produced
   * @return the result of the union
   */
  compact_theta_sketch_alloc<A> get_result(bool ordered = true) const;

private:
  bool is_empty_;
  uint64_t theta_;
  update_theta_sketch_alloc<A> state_;

  // for builder
  theta_union_alloc(uint64_t theta, update_theta_sketch_alloc<A>&& state);
};

// builder

template<typename A>
class theta_union_alloc<A>::builder {
public:
  typedef typename update_theta_sketch_alloc<A>::resize_factor resize_factor;

  /**
   * Set log2(k), where k is a nominal number of entries in the sketch
   * @param lg_k base 2 logarithm of nominal number of entries
   * @return this builder
   */
  builder& set_lg_k(uint8_t lg_k);

  /**
   * Set resize factor for the internal hash table (defaults to 8)
   * @param rf resize factor
   * @return this builder
   */
  builder& set_resize_factor(resize_factor rf);

  /**
   * Set sampling probability (initial theta). The default is 1, so the sketch retains
   * all entries until it reaches the limit, at which point it goes into the estimation mode
   * and reduces the effective sampling probability (theta) as necessary.
   * @param p sampling probability
   * @return this builder
   */
  builder& set_p(float p);

  /**
   * Set the seed for the hash function. Should be used carefully if needed.
   * Sketches produced with different seed are not compatible
   * and cannot be mixed in set operations.
   * @param seed hash seed
   * @return this builder
   */
  builder& set_seed(uint64_t seed);

  /**
   * This is to create an instance of the union with predefined parameters.
   * @return and instance of the union
   */
  theta_union_alloc<A> build() const;

private:
  typename update_theta_sketch_alloc<A>::builder sketch_builder;
};

// alias with default allocator for convenience
typedef theta_union_alloc<std::allocator<void>> theta_union;

} /* namespace datasketches */

#include "theta_union_impl.hpp"

# endif
