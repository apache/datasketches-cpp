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

#ifndef THETA_SKETCH_HPP_
#define THETA_SKETCH_HPP_

#include <memory>
#include <functional>
#include <climits>

namespace datasketches {

/*
 * author Alexander Saydakov
 * author Lee Rhodes
 * author Kevin Lang
 */

// forward-declarations
template<typename A> class theta_sketch_alloc;
template<typename A> class update_theta_sketch_alloc;
template<typename A> class compact_theta_sketch_alloc;
template<typename A> class theta_union_alloc;
template<typename A> class theta_intersection_alloc;
template<typename A> class theta_a_not_b_alloc;

// for serialization as raw bytes
typedef std::unique_ptr<void, std::function<void(void*)>> void_ptr_with_deleter;

template<typename A>
class theta_sketch_alloc {
public:
  static const uint64_t MAX_THETA = LLONG_MAX; // signed max for compatibility with Java
  static const uint8_t SERIAL_VERSION = 3;

  theta_sketch_alloc(bool is_empty, uint64_t theta);
  theta_sketch_alloc(const theta_sketch_alloc<A>& other);
  theta_sketch_alloc(theta_sketch_alloc<A>&& other) noexcept;
  virtual ~theta_sketch_alloc();

  theta_sketch_alloc<A>& operator=(const theta_sketch_alloc<A>& other);
  theta_sketch_alloc<A>& operator=(theta_sketch_alloc<A>&& other);

  bool is_empty() const;
  double get_estimate() const;
  double get_lower_bound(uint8_t num_std_devs) const;
  double get_upper_bound(uint8_t num_std_devs) const;
  bool is_estimation_mode() const;
  double get_theta() const;
  uint64_t get_theta64() const;

  virtual uint32_t get_num_retained() const = 0;
  virtual uint16_t get_seed_hash() const = 0;
  virtual bool is_ordered() const = 0;
  virtual void to_stream(std::ostream& os, bool print_items = false) const = 0;
  virtual void serialize(std::ostream& os) const = 0;
  virtual std::pair<void_ptr_with_deleter, const size_t> serialize(unsigned header_size_bytes = 0) const = 0;

  typedef std::unique_ptr<theta_sketch_alloc<A>, std::function<void(theta_sketch_alloc<A>*)>> unique_ptr;
  static unique_ptr deserialize(std::istream& is, uint64_t seed = update_theta_sketch_alloc<A>::builder::DEFAULT_SEED);
  static unique_ptr deserialize(const void* bytes, size_t size, uint64_t seed = update_theta_sketch_alloc<A>::builder::DEFAULT_SEED);

  class const_iterator;
  virtual const_iterator begin() const = 0;
  virtual const_iterator end() const = 0;

protected:
  enum flags { IS_BIG_ENDIAN, IS_READ_ONLY, IS_EMPTY, IS_COMPACT, IS_ORDERED };

  bool is_empty_;
  uint64_t theta_;

  static uint16_t get_seed_hash(uint64_t seed);

  static void check_sketch_type(uint8_t actual, uint8_t expected);
  static void check_serial_version(uint8_t actual, uint8_t expected);
  static void check_seed_hash(uint16_t actual, uint16_t expected);
  static void check_size(size_t actual, size_t expected);

  friend theta_intersection_alloc<A>;
  friend theta_a_not_b_alloc<A>;
};

// update sketch

template<typename A>
class update_theta_sketch_alloc: public theta_sketch_alloc<A> {
public:
  class builder;
  enum resize_factor { X1, X2, X4, X8 };
  static const uint8_t SKETCH_TYPE = 2;

  update_theta_sketch_alloc(const update_theta_sketch_alloc<A>& other);
  update_theta_sketch_alloc(update_theta_sketch_alloc<A>&& other) noexcept;
  virtual ~update_theta_sketch_alloc();

  update_theta_sketch_alloc<A>& operator=(const update_theta_sketch_alloc<A>& other);
  update_theta_sketch_alloc<A>& operator=(update_theta_sketch_alloc<A>&& other);

  virtual uint32_t get_num_retained() const;
  virtual uint16_t get_seed_hash() const;
  virtual bool is_ordered() const;
  virtual void to_stream(std::ostream& os, bool print_items = false) const;
  virtual void serialize(std::ostream& os) const;
  // header space is reserved, but not initialized
  virtual std::pair<void_ptr_with_deleter, const size_t> serialize(unsigned header_size_bytes = 0) const;

  void update(const std::string& value);
  void update(uint64_t value);
  void update(int64_t value);

  // for compatibility with Java implementation
  void update(uint32_t value);
  void update(int32_t value);
  void update(uint16_t value);
  void update(int16_t value);
  void update(uint8_t value);
  void update(int8_t value);
  void update(double value);
  void update(float value);

  // Be very careful to hash input values consistently using the same approach
  // either over time or on different platforms
  // or while passing sketches from Java environment or to Java environment
  // Otherwise two sketches that should represent overlapping sets will be disjoint
  // For instance, for signed 32-bit values call update(int32_t) method above,
  // which does widening conversion to int64_t, if compatibility with Java is expected
  void update(const void* data, unsigned length);

  compact_theta_sketch_alloc<A> compact(bool ordered = true) const;

  virtual typename theta_sketch_alloc<A>::const_iterator begin() const;
  virtual typename theta_sketch_alloc<A>::const_iterator end() const;

  static update_theta_sketch_alloc<A> deserialize(std::istream& is, uint64_t seed = builder::DEFAULT_SEED);
  static update_theta_sketch_alloc<A> deserialize(const void* bytes, size_t size, uint64_t seed = update_theta_sketch_alloc<A>::builder::DEFAULT_SEED);

private:
  // resize threshold = 0.5 tuned for speed
  static constexpr double RESIZE_THRESHOLD = 0.5;
  // hash table rebuild threshold = 15/16
  static constexpr double REBUILD_THRESHOLD = 15.0 / 16.0;

  static constexpr uint8_t STRIDE_HASH_BITS = 7;
  static constexpr uint32_t STRIDE_MASK = (1 << STRIDE_HASH_BITS) - 1;

  uint8_t lg_cur_size_;
  uint8_t lg_nom_size_;
  uint64_t* keys_;
  uint32_t num_keys_;
  resize_factor rf_;
  float p_;
  uint64_t seed_;
  uint32_t capacity_;

  typedef typename std::allocator_traits<A>::template rebind_alloc<uint64_t> AllocU64;

  // for builder
  update_theta_sketch_alloc(uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf, float p, uint64_t seed);
  // for deserialize
  update_theta_sketch_alloc(bool is_empty, uint64_t theta, uint8_t lg_cur_size, uint8_t lg_nom_size, uint64_t* keys, uint32_t num_keys, resize_factor rf, float p, uint64_t seed);

  void resize();
  void rebuild();

  friend theta_union_alloc<A>;
  void internal_update(uint64_t hash);

  friend theta_intersection_alloc<A>;
  friend theta_a_not_b_alloc<A>;
  static inline uint32_t get_capacity(uint8_t lg_cur_size, uint8_t lg_nom_size);
  static inline uint32_t get_stride(uint64_t hash, uint8_t lg_size);
  static bool hash_search_or_insert(uint64_t hash, uint64_t* table, uint8_t lg_size);
  static bool hash_search(uint64_t hash, const uint64_t* table, uint8_t lg_size);

  friend theta_sketch_alloc<A>;
  static update_theta_sketch_alloc<A> internal_deserialize(std::istream& is, resize_factor rf, uint8_t lg_nom_size, uint8_t lg_cur_size, uint8_t flags_byte, uint64_t seed);
  static update_theta_sketch_alloc<A> internal_deserialize(const void* bytes, size_t size, resize_factor rf, uint8_t lg_nom_size, uint8_t lg_cur_size, uint8_t flags_byte, uint64_t seed);
};

// compact sketch

template<typename A>
class compact_theta_sketch_alloc: public theta_sketch_alloc<A> {
public:
  static const uint8_t SKETCH_TYPE = 3;

  compact_theta_sketch_alloc(const compact_theta_sketch_alloc<A>& other);
  compact_theta_sketch_alloc(const theta_sketch_alloc<A>& other, bool ordered);
  compact_theta_sketch_alloc(compact_theta_sketch_alloc<A>&& other) noexcept;
  virtual ~compact_theta_sketch_alloc();

  compact_theta_sketch_alloc<A>& operator=(const compact_theta_sketch_alloc<A>& other);
  compact_theta_sketch_alloc<A>& operator=(compact_theta_sketch_alloc<A>&& other);

  virtual uint32_t get_num_retained() const;
  virtual uint16_t get_seed_hash() const;
  virtual bool is_ordered() const;
  virtual void to_stream(std::ostream& os, bool print_items = false) const;
  virtual void serialize(std::ostream& os) const;
  // header space is reserved, but not initialized
  virtual std::pair<void_ptr_with_deleter, const size_t> serialize(unsigned header_size_bytes = 0) const;

  virtual typename theta_sketch_alloc<A>::const_iterator begin() const;
  virtual typename theta_sketch_alloc<A>::const_iterator end() const;

  static compact_theta_sketch_alloc<A> deserialize(std::istream& is, uint64_t seed = update_theta_sketch_alloc<A>::builder::DEFAULT_SEED);
  static compact_theta_sketch_alloc<A> deserialize(const void* bytes, size_t size, uint64_t seed = update_theta_sketch_alloc<A>::builder::DEFAULT_SEED);

private:
  typedef typename std::allocator_traits<A>::template rebind_alloc<uint64_t> AllocU64;

  uint64_t* keys_;
  uint32_t num_keys_;
  uint16_t seed_hash_;
  bool is_ordered_;

  friend theta_sketch_alloc<A>;
  friend update_theta_sketch_alloc<A>;
  friend theta_union_alloc<A>;
  friend theta_intersection_alloc<A>;
  friend theta_a_not_b_alloc<A>;
  compact_theta_sketch_alloc(bool is_empty, uint64_t theta, uint64_t* keys, uint32_t num_keys, uint16_t seed_hash, bool is_ordered);
  static compact_theta_sketch_alloc<A> internal_deserialize(std::istream& is, uint8_t preamble_longs, uint8_t flags_byte, uint16_t seed_hash);
  static compact_theta_sketch_alloc<A> internal_deserialize(const void* bytes, size_t size, uint8_t preamble_longs, uint8_t flags_byte, uint16_t seed_hash);
};

// builder

template<typename A>
class update_theta_sketch_alloc<A>::builder {
public:
  static const uint8_t MIN_LG_K = 5;
  static const uint8_t DEFAULT_LG_K = 12;
  static const resize_factor DEFAULT_RESIZE_FACTOR = X8;
  static const uint64_t DEFAULT_SEED = 9001;

  builder();
  builder& set_lg_k(uint8_t lg_k);
  builder& set_resize_factor(resize_factor rf);
  builder& set_p(float p);
  builder& set_seed(uint64_t seed);
  update_theta_sketch_alloc<A> build() const;
private:
  uint8_t lg_k_;
  resize_factor rf_;
  float p_;
  uint64_t seed_;

  static uint8_t starting_sub_multiple(uint8_t lg_tgt, uint8_t lg_min, uint8_t lg_rf);
};

// iterator
template<typename A>
class theta_sketch_alloc<A>::const_iterator: public std::iterator<std::input_iterator_tag, uint64_t> {
public:
  const_iterator& operator++();
  const_iterator operator++(int);
  bool operator==(const const_iterator& other) const;
  bool operator!=(const const_iterator& other) const;
  uint64_t operator*() const;

private:
  const uint64_t* keys_;
  uint32_t size_;
  uint32_t index_;
  const_iterator(const uint64_t* keys, uint32_t size, uint32_t index);
  friend class update_theta_sketch_alloc<A>;
  friend class compact_theta_sketch_alloc<A>;
};


// aliases with default allocator for convenience
typedef theta_sketch_alloc<std::allocator<void>> theta_sketch;
typedef update_theta_sketch_alloc<std::allocator<void>> update_theta_sketch;
typedef compact_theta_sketch_alloc<std::allocator<void>> compact_theta_sketch;

// common helping functions

constexpr uint8_t log2(uint32_t n) {
  return (n > 1) ? 1 + log2(n >> 1) : 0;
}

constexpr uint8_t lg_size_from_count(uint32_t n, double load_factor) {
  return log2(n) + ((n > static_cast<uint32_t>((1 << (log2(n) + 1)) * load_factor)) ? 2 : 1);
}

} /* namespace datasketches */

#include "theta_sketch_impl.hpp"

# endif
