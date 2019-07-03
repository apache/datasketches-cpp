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

#ifndef THETA_SKETCH_IMPL_HPP_
#define THETA_SKETCH_IMPL_HPP_

#include <algorithm>
#include <cmath>
#include <memory>
#include <functional>
#include <istream>
#include <ostream>

#include "MurmurHash3.h"
#include "serde.hpp"
#include "binomial_bounds.hpp"

namespace datasketches {

/*
 * author Alexander Saydakov
 * author Lee Rhodes
 * author Kevin Lang
 */

template<typename A>
theta_sketch_alloc<A>::theta_sketch_alloc(bool is_empty, uint64_t theta):
is_empty_(is_empty), theta_(theta)
{}

template<typename A>
theta_sketch_alloc<A>::theta_sketch_alloc(const theta_sketch_alloc<A>& other):
is_empty_(other.is_empty_), theta_(other.theta_)
{}

template<typename A>
theta_sketch_alloc<A>::theta_sketch_alloc(theta_sketch_alloc<A>&& other) noexcept:
is_empty_(other.is_empty_), theta_(other.theta_)
{}

template<typename A>
theta_sketch_alloc<A>& theta_sketch_alloc<A>::operator=(const theta_sketch_alloc<A>& other) {
  is_empty_ = other.is_empty_;
  theta_ = other.theta_;
  return *this;
}

template<typename A>
theta_sketch_alloc<A>& theta_sketch_alloc<A>::operator=(theta_sketch_alloc<A>&& other) {
  std::swap(is_empty_, other.is_empty_);
  std::swap(theta_, other.theta_);
  return *this;
}

template<typename A>
theta_sketch_alloc<A>::~theta_sketch_alloc() {
}

template<typename A>
bool theta_sketch_alloc<A>::is_empty() const {
  return is_empty_;
}

template<typename A>
double theta_sketch_alloc<A>::get_estimate() const {
  return get_num_retained() / get_theta();
}

template<typename A>
double theta_sketch_alloc<A>::get_lower_bound(uint8_t num_std_devs) const {
  if (!is_estimation_mode()) return get_num_retained();
  return binomial_bounds::get_lower_bound(get_num_retained(), get_theta(), num_std_devs);
}

template<typename A>
double theta_sketch_alloc<A>::get_upper_bound(uint8_t num_std_devs) const {
  if (!is_estimation_mode()) return get_num_retained();
  return binomial_bounds::get_upper_bound(get_num_retained(), get_theta(), num_std_devs);
}

template<typename A>
bool theta_sketch_alloc<A>::is_estimation_mode() const {
  return theta_ < MAX_THETA and !is_empty_;
}

template<typename A>
double theta_sketch_alloc<A>::get_theta() const {
  return (double) theta_ / MAX_THETA;
}

template<typename A>
uint64_t theta_sketch_alloc<A>::get_theta64() const {
  return theta_;
}

template<typename A>
typename theta_sketch_alloc<A>::unique_ptr theta_sketch_alloc<A>::deserialize(std::istream& is, uint64_t seed) {
  uint8_t preamble_longs;
  is.read((char*)&preamble_longs, sizeof(preamble_longs));
  uint8_t serial_version;
  is.read((char*)&serial_version, sizeof(serial_version));
  uint8_t type;
  is.read((char*)&type, sizeof(type));
  uint8_t lg_nom_size;
  is.read((char*)&lg_nom_size, sizeof(lg_nom_size));
  uint8_t lg_cur_size;
  is.read((char*)&lg_cur_size, sizeof(lg_cur_size));
  uint8_t flags_byte;
  is.read((char*)&flags_byte, sizeof(flags_byte));
  uint16_t seed_hash;
  is.read((char*)&seed_hash, sizeof(seed_hash));

  check_serial_version(serial_version, SERIAL_VERSION);
  check_seed_hash(seed_hash, get_seed_hash(seed));

  if (type == update_theta_sketch_alloc<A>::SKETCH_TYPE) {
    typename update_theta_sketch_alloc<A>::resize_factor rf = static_cast<typename update_theta_sketch_alloc<A>::resize_factor>(preamble_longs >> 6);
    typedef typename std::allocator_traits<A>::template rebind_alloc<update_theta_sketch_alloc<A>> AU;
    return unique_ptr(
      static_cast<theta_sketch_alloc<A>*>(new (AU().allocate(1)) update_theta_sketch_alloc<A>(update_theta_sketch_alloc<A>::internal_deserialize(is, rf, lg_nom_size, lg_cur_size, flags_byte, seed))),
      [](theta_sketch_alloc<A>* ptr) {
        ptr->~theta_sketch_alloc();
        AU().deallocate(static_cast<update_theta_sketch_alloc<A>*>(ptr), 1);
      }
    );
  } else if (type == compact_theta_sketch_alloc<A>::SKETCH_TYPE) {
    typedef typename std::allocator_traits<A>::template rebind_alloc<compact_theta_sketch_alloc<A>> AC;
    return unique_ptr(
      static_cast<theta_sketch_alloc<A>*>(new (AC().allocate(1)) compact_theta_sketch_alloc<A>(compact_theta_sketch_alloc<A>::internal_deserialize(is, preamble_longs, flags_byte, seed_hash))),
      [](theta_sketch_alloc<A>* ptr) {
        ptr->~theta_sketch_alloc();
        AC().deallocate(static_cast<compact_theta_sketch_alloc<A>*>(ptr), 1);
      }
    );
  }
  throw std::invalid_argument("unsupported sketch type " + std::to_string((int) type));
}

template<typename A>
typename theta_sketch_alloc<A>::unique_ptr theta_sketch_alloc<A>::deserialize(const void* bytes, size_t size, uint64_t seed) {
  check_size(size, static_cast<size_t>(8));
  const char* ptr = static_cast<const char*>(bytes);
  uint8_t preamble_longs;
  copy_from_mem(&ptr, &preamble_longs, sizeof(preamble_longs));
  uint8_t serial_version;
  copy_from_mem(&ptr, &serial_version, sizeof(serial_version));
  uint8_t type;
  copy_from_mem(&ptr, &type, sizeof(type));
  uint8_t lg_nom_size;
  copy_from_mem(&ptr, &lg_nom_size, sizeof(lg_nom_size));
  uint8_t lg_cur_size;
  copy_from_mem(&ptr, &lg_cur_size, sizeof(lg_cur_size));
  uint8_t flags_byte;
  copy_from_mem(&ptr, &flags_byte, sizeof(flags_byte));
  uint16_t seed_hash;
  copy_from_mem(&ptr, &seed_hash, sizeof(seed_hash));

  check_serial_version(serial_version, SERIAL_VERSION);
  check_seed_hash(seed_hash, get_seed_hash(seed));

  if (type == update_theta_sketch_alloc<A>::SKETCH_TYPE) {
    typename update_theta_sketch_alloc<A>::resize_factor rf = static_cast<typename update_theta_sketch_alloc<A>::resize_factor>(preamble_longs >> 6);
    typedef typename std::allocator_traits<A>::template rebind_alloc<update_theta_sketch_alloc<A>> AU;
    return unique_ptr(
      static_cast<theta_sketch_alloc<A>*>(new (AU().allocate(1)) update_theta_sketch_alloc<A>(
        update_theta_sketch_alloc<A>::internal_deserialize(ptr, size - (ptr - static_cast<const char*>(bytes)), rf, lg_nom_size, lg_cur_size, flags_byte, seed))
      ),
      [](theta_sketch_alloc<A>* ptr) {
        ptr->~theta_sketch_alloc();
        AU().deallocate(static_cast<update_theta_sketch_alloc<A>*>(ptr), 1);
      }
    );
  } else if (type == compact_theta_sketch_alloc<A>::SKETCH_TYPE) {
    typedef typename std::allocator_traits<A>::template rebind_alloc<compact_theta_sketch_alloc<A>> AC;
    return unique_ptr(
      static_cast<theta_sketch_alloc<A>*>(new (AC().allocate(1)) compact_theta_sketch_alloc<A>(
        compact_theta_sketch_alloc<A>::internal_deserialize(ptr, size - (ptr - static_cast<const char*>(bytes)), preamble_longs, flags_byte, seed_hash))
      ),
      [](theta_sketch_alloc<A>* ptr) {
        ptr->~theta_sketch_alloc();
        AC().deallocate(static_cast<compact_theta_sketch_alloc<A>*>(ptr), 1);
      }
    );
  }
  throw std::invalid_argument("unsupported sketch type " + std::to_string((int) type));
}

template<typename A>
uint16_t theta_sketch_alloc<A>::get_seed_hash(uint64_t seed) {
  HashState hashes;
  MurmurHash3_x64_128(&seed, sizeof(seed), 0, hashes);
  return hashes.h1;
}

template<typename A>
void theta_sketch_alloc<A>::check_sketch_type(uint8_t actual, uint8_t expected) {
  if (actual != expected) {
    throw std::invalid_argument("Sketch type mismatch: expected " + std::to_string((int)expected) + ", actual " + std::to_string((int)actual));
  }
}

template<typename A>
void theta_sketch_alloc<A>::check_serial_version(uint8_t actual, uint8_t expected) {
  if (actual != expected) {
    throw std::invalid_argument("Sketch serial version mismatch: expected " + std::to_string((int)expected) + ", actual " + std::to_string((int)actual));
  }
}

template<typename A>
void theta_sketch_alloc<A>::check_seed_hash(uint16_t actual, uint16_t expected) {
  if (actual != expected) {
    throw std::invalid_argument("Sketch seed hash mismatch: expected " + std::to_string(expected) + ", actual " + std::to_string(actual));
  }
}

template<typename A>
void theta_sketch_alloc<A>::check_size(size_t actual, size_t expected) {
  if (actual < expected) {
    throw std::invalid_argument("Given memory is smaller than expected: expected " + std::to_string((int)expected) + ", actual " + std::to_string((int) actual));
  }
}

// update sketch

template<typename A>
update_theta_sketch_alloc<A>::update_theta_sketch_alloc(uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf, float p, uint64_t seed):
theta_sketch_alloc<A>(true, theta_sketch_alloc<A>::MAX_THETA),
lg_cur_size_(lg_cur_size),
lg_nom_size_(lg_nom_size),
keys_(AllocU64().allocate(1 << lg_cur_size_)),
num_keys_(0),
rf_(rf),
p_(p),
seed_(seed),
capacity_(get_capacity(lg_cur_size, lg_nom_size))
{
  if (p < 1) this->theta_ *= p;
  std::fill(keys_, &keys_[1 << lg_cur_size_], 0);
}

template<typename A>
update_theta_sketch_alloc<A>::update_theta_sketch_alloc(bool is_empty, uint64_t theta, uint8_t lg_cur_size, uint8_t lg_nom_size, uint64_t* keys, uint32_t num_keys, resize_factor rf, float p, uint64_t seed):
theta_sketch_alloc<A>(is_empty, theta),
lg_cur_size_(lg_cur_size),
lg_nom_size_(lg_nom_size),
keys_(keys),
num_keys_(num_keys),
rf_(rf),
p_(p),
seed_(seed),
capacity_(get_capacity(lg_cur_size, lg_nom_size))
{}

template<typename A>
update_theta_sketch_alloc<A>::update_theta_sketch_alloc(const update_theta_sketch_alloc<A>& other):
theta_sketch_alloc<A>(other),
lg_cur_size_(other.lg_cur_size_),
lg_nom_size_(other.lg_nom_size_),
keys_(AllocU64().allocate(1 << lg_cur_size_)),
num_keys_(other.num_keys_),
rf_(other.rf_),
p_(other.p_),
seed_(other.seed_),
capacity_(other.capacity_)
{
  std::copy(other.keys_, &other.keys_[1 << lg_cur_size_], keys_);
}

template<typename A>
update_theta_sketch_alloc<A>::update_theta_sketch_alloc(update_theta_sketch_alloc<A>&& other) noexcept:
theta_sketch_alloc<A>(std::move(other)),
lg_cur_size_(other.lg_cur_size_),
lg_nom_size_(other.lg_nom_size_),
keys_(nullptr),
num_keys_(other.num_keys_),
rf_(other.rf_),
p_(other.p_),
seed_(other.seed_),
capacity_(other.capacity_)
{
  std::swap(keys_, other.keys_);
}

template<typename A>
update_theta_sketch_alloc<A>::~update_theta_sketch_alloc() {
  AllocU64().deallocate(keys_, 1 << lg_cur_size_);
}

template<typename A>
update_theta_sketch_alloc<A>& update_theta_sketch_alloc<A>::operator=(const update_theta_sketch_alloc<A>& other) {
  theta_sketch_alloc<A>::operator=(other);
  if (lg_cur_size_ != other.lg_cur_size_) {
    AllocU64().deallocate(keys_, 1 << lg_cur_size_);
    lg_cur_size_ = other.lg_cur_size_;
    keys_ = AllocU64().allocate(1 << lg_cur_size_);
  }
  lg_nom_size_ = other.lg_nom_size_;
  std::copy(other.keys_, &other.keys_[1 << lg_cur_size_], keys_);
  num_keys_ = other.num_keys_;
  rf_ = other.rf_;
  p_ = other.p_;
  seed_ = other.seed_;
  capacity_ = other.capacity_;
  return *this;
}

template<typename A>
update_theta_sketch_alloc<A>& update_theta_sketch_alloc<A>::operator=(update_theta_sketch_alloc<A>&& other) {
  theta_sketch_alloc<A>::operator=(std::move(other));
  std::swap(lg_cur_size_, other.lg_cur_size_);
  lg_nom_size_ = other.lg_nom_size_;
  std::swap(keys_, other.keys_);
  num_keys_ = other.num_keys_;
  rf_ = other.rf_;
  p_ = other.p_;
  seed_ = other.seed_;
  capacity_ = other.capacity_;
  return *this;
}

template<typename A>
uint32_t update_theta_sketch_alloc<A>::get_num_retained() const {
  return num_keys_;
}

template<typename A>
uint16_t update_theta_sketch_alloc<A>::get_seed_hash() const {
  return theta_sketch_alloc<A>::get_seed_hash(seed_);
}

template<typename A>
bool update_theta_sketch_alloc<A>::is_ordered() const {
  return false;
}

template<typename A>
void update_theta_sketch_alloc<A>::to_stream(std::ostream& os, bool print_items) const {
  os << "### Update Theta sketch summary:" << std::endl;
  os << "   lg nominal size      : " << (int) lg_nom_size_ << std::endl;
  os << "   lg current size      : " << (int) lg_cur_size_ << std::endl;
  os << "   num retained keys    : " << num_keys_ << std::endl;
  os << "   resize factor        : " << (1 << rf_) << std::endl;
  os << "   sampling probability : " << p_ << std::endl;
  os << "   seed hash            : " << this->get_seed_hash() << std::endl;
  os << "   ordered?             : " << (this->is_ordered() ? "true" : "false") << std::endl;
  os << "   theta (fraction)     : " << this->get_theta() << std::endl;
  os << "   theta (raw 64-bit)   : " << this->theta_ << std::endl;
  os << "   estimation mode?     : " << (this->is_estimation_mode() ? "true" : "false") << std::endl;
  os << "   estimate             : " << this->get_estimate() << std::endl;
  os << "   lower bound 95% conf : " << this->get_lower_bound(2) << std::endl;
  os << "   upper bound 95% conf : " << this->get_upper_bound(2) << std::endl;
  os << "### End sketch summary" << std::endl;
  if (print_items) {
    os << "### Retained keys" << std::endl;
    for (auto key: *this) os << "   " << key << std::endl;
    os << "### End retained keys" << std::endl;
  }
}

template<typename A>
void update_theta_sketch_alloc<A>::serialize(std::ostream& os) const {
  const uint8_t preamble_longs_and_rf = 3 | (rf_ << 6);
  os.write((char*)&preamble_longs_and_rf, sizeof(preamble_longs_and_rf));
  const uint8_t serial_version = theta_sketch_alloc<A>::SERIAL_VERSION;
  os.write((char*)&serial_version, sizeof(serial_version));
  const uint8_t type = SKETCH_TYPE;
  os.write((char*)&type, sizeof(type));
  os.write((char*)&lg_nom_size_, sizeof(lg_nom_size_));
  os.write((char*)&lg_cur_size_, sizeof(lg_cur_size_));
  const uint8_t flags_byte(
    (this->is_empty() ? 1 << theta_sketch_alloc<A>::flags::IS_EMPTY : 0)
  );
  os.write((char*)&flags_byte, sizeof(flags_byte));
  const uint16_t seed_hash = get_seed_hash();
  os.write((char*)&seed_hash, sizeof(seed_hash));
  os.write((char*)&num_keys_, sizeof(num_keys_));
  os.write((char*)&p_, sizeof(p_));
  os.write((char*)&(this->theta_), sizeof(uint64_t));
  os.write((char*)keys_, sizeof(uint64_t) * (1 << lg_cur_size_));
}

template<typename A>
std::pair<void_ptr_with_deleter, const size_t> update_theta_sketch_alloc<A>::serialize(unsigned header_size_bytes) const {
  const uint8_t preamble_longs = 3;
  const size_t size = header_size_bytes + sizeof(uint64_t) * preamble_longs + sizeof(uint64_t) * (1 << lg_cur_size_);
  typedef typename std::allocator_traits<A>::template rebind_alloc<char> AllocChar;
  void_ptr_with_deleter data_ptr(
    static_cast<void*>(AllocChar().allocate(size)),
    [size](void* ptr) { AllocChar().deallocate(static_cast<char*>(ptr), size); }
  );
  char* ptr = static_cast<char*>(data_ptr.get()) + header_size_bytes;

  const uint8_t preamble_longs_and_rf = preamble_longs | (rf_ << 6);
  copy_to_mem(&preamble_longs_and_rf, &ptr, sizeof(preamble_longs_and_rf));
  const uint8_t serial_version = theta_sketch_alloc<A>::SERIAL_VERSION;
  copy_to_mem(&serial_version, &ptr, sizeof(serial_version));
  const uint8_t type = SKETCH_TYPE;
  copy_to_mem(&type, &ptr, sizeof(type));
  copy_to_mem(&lg_nom_size_, &ptr, sizeof(lg_nom_size_));
  copy_to_mem(&lg_cur_size_, &ptr, sizeof(lg_cur_size_));
  const uint8_t flags_byte(
    (this->is_empty() ? 1 << theta_sketch_alloc<A>::flags::IS_EMPTY : 0)
  );
  copy_to_mem(&flags_byte, &ptr, sizeof(flags_byte));
  const uint16_t seed_hash = get_seed_hash();
  copy_to_mem(&seed_hash, &ptr, sizeof(seed_hash));
  copy_to_mem(&num_keys_, &ptr, sizeof(num_keys_));
  copy_to_mem(&p_, &ptr, sizeof(p_));
  copy_to_mem(&(this->theta_), &ptr, sizeof(uint64_t));
  copy_to_mem(keys_, &ptr, sizeof(uint64_t) * (1 << lg_cur_size_));

  return std::make_pair(std::move(data_ptr), size);;
}

template<typename A>
update_theta_sketch_alloc<A> update_theta_sketch_alloc<A>::deserialize(std::istream& is, uint64_t seed) {
  uint8_t preamble_longs;
  is.read((char*)&preamble_longs, sizeof(preamble_longs));
  resize_factor rf = static_cast<resize_factor>(preamble_longs >> 6);
  preamble_longs &= 0x3f; // remove resize factor
  uint8_t serial_version;
  is.read((char*)&serial_version, sizeof(serial_version));
  uint8_t type;
  is.read((char*)&type, sizeof(type));
  uint8_t lg_nom_size;
  is.read((char*)&lg_nom_size, sizeof(lg_nom_size));
  uint8_t lg_cur_size;
  is.read((char*)&lg_cur_size, sizeof(lg_cur_size));
  uint8_t flags_byte;
  is.read((char*)&flags_byte, sizeof(flags_byte));
  uint16_t seed_hash;
  is.read((char*)&seed_hash, sizeof(seed_hash));
  theta_sketch_alloc<A>::check_sketch_type(type, SKETCH_TYPE);
  theta_sketch_alloc<A>::check_serial_version(serial_version, theta_sketch_alloc<A>::SERIAL_VERSION);
  theta_sketch_alloc<A>::check_seed_hash(seed_hash, theta_sketch_alloc<A>::get_seed_hash(seed));
  return internal_deserialize(is, rf, lg_nom_size, lg_cur_size, flags_byte, seed);
}

template<typename A>
update_theta_sketch_alloc<A> update_theta_sketch_alloc<A>::internal_deserialize(std::istream& is, resize_factor rf, uint8_t lg_nom_size, uint8_t lg_cur_size, uint8_t flags_byte, uint64_t seed) {
  uint32_t num_keys;
  is.read((char*)&num_keys, sizeof(num_keys));
  float p;
  is.read((char*)&p, sizeof(p));
  uint64_t theta;
  is.read((char*)&theta, sizeof(theta));
  uint64_t* keys = AllocU64().allocate(1 << lg_cur_size);
  is.read((char*)keys, sizeof(uint64_t) * (1 << lg_cur_size));
  const bool is_empty = flags_byte & (1 << theta_sketch_alloc<A>::flags::IS_EMPTY);
  return update_theta_sketch_alloc<A>(is_empty, theta, lg_nom_size, lg_cur_size, keys, num_keys, rf, p, seed);
}

template<typename A>
update_theta_sketch_alloc<A> update_theta_sketch_alloc<A>::deserialize(const void* bytes, size_t size, uint64_t seed) {
  theta_sketch_alloc<A>::check_size(size, 8);
  const char* ptr = static_cast<const char*>(bytes);
  uint8_t preamble_longs;
  copy_from_mem(&ptr, &preamble_longs, sizeof(preamble_longs));
  resize_factor rf = static_cast<resize_factor>(preamble_longs >> 6);
  preamble_longs &= 0x3f; // remove resize factor
  uint8_t serial_version;
  copy_from_mem(&ptr, &serial_version, sizeof(serial_version));
  uint8_t type;
  copy_from_mem(&ptr, &type, sizeof(type));
  uint8_t lg_nom_size;
  copy_from_mem(&ptr, &lg_nom_size, sizeof(lg_nom_size));
  uint8_t lg_cur_size;
  copy_from_mem(&ptr, &lg_cur_size, sizeof(lg_cur_size));
  uint8_t flags_byte;
  copy_from_mem(&ptr, &flags_byte, sizeof(flags_byte));
  uint16_t seed_hash;
  copy_from_mem(&ptr, &seed_hash, sizeof(seed_hash));
  theta_sketch_alloc<A>::check_sketch_type(type, SKETCH_TYPE);
  theta_sketch_alloc<A>::check_serial_version(serial_version, theta_sketch_alloc<A>::SERIAL_VERSION);
  theta_sketch_alloc<A>::check_seed_hash(seed_hash, theta_sketch_alloc<A>::get_seed_hash(seed));
  return internal_deserialize(ptr, size - (ptr - static_cast<const char*>(bytes)), rf, lg_nom_size, lg_cur_size, flags_byte, seed);
}

template<typename A>
update_theta_sketch_alloc<A> update_theta_sketch_alloc<A>::internal_deserialize(const void* bytes, size_t size, resize_factor rf, uint8_t lg_nom_size, uint8_t lg_cur_size, uint8_t flags_byte, uint64_t seed) {
  const uint32_t table_size = 1 << lg_cur_size;
  theta_sketch_alloc<A>::check_size(size, 16 + sizeof(uint64_t) * table_size);
  const char* ptr = static_cast<const char*>(bytes);
  uint32_t num_keys;
  copy_from_mem(&ptr, &num_keys, sizeof(num_keys));
  float p;
  copy_from_mem(&ptr, &p, sizeof(p));
  uint64_t theta;
  copy_from_mem(&ptr, &theta, sizeof(theta));
  uint64_t* keys = AllocU64().allocate(table_size);
  copy_from_mem(&ptr, keys, sizeof(uint64_t) * table_size);
  const bool is_empty = flags_byte & (1 << theta_sketch_alloc<A>::flags::IS_EMPTY);
  return update_theta_sketch_alloc<A>(is_empty, theta, lg_nom_size, lg_cur_size, keys, num_keys, rf, p, seed);
}

template<typename A>
void update_theta_sketch_alloc<A>::update(const std::string& value) {
  if (value.empty()) return;
  update(value.c_str(), value.length());
}

template<typename A>
void update_theta_sketch_alloc<A>::update(uint64_t value) {
  update(&value, sizeof(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(int64_t value) {
  update(&value, sizeof(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(uint32_t value) {
  update(static_cast<int32_t>(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(int32_t value) {
  update(static_cast<int64_t>(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(uint16_t value) {
  update(static_cast<int16_t>(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(int16_t value) {
  update(static_cast<int64_t>(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(uint8_t value) {
  update(static_cast<int8_t>(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(int8_t value) {
  update(static_cast<int64_t>(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(double value) {
  union {
    int64_t long_value;
    double double_value;
  } long_double_union;

  if (value == 0.0) {
    long_double_union.double_value = 0.0; // canonicalize -0.0 to 0.0
  } else if (std::isnan(value)) {
    long_double_union.long_value = 0x7ff8000000000000L; // canonicalize NaN using value from Java's Double.doubleToLongBits()
  } else {
    long_double_union.double_value = value;
  }
  update(&long_double_union, sizeof(long_double_union));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(float value) {
  update(static_cast<double>(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(const void* data, unsigned length) {
  HashState hashes;
  MurmurHash3_x64_128(data, length, seed_, hashes);
  const uint64_t hash = hashes.h1 >> 1; // Java implementation does logical shift >>> to make values positive
  internal_update(hash);
}

template<typename A>
compact_theta_sketch_alloc<A> update_theta_sketch_alloc<A>::compact(bool ordered) const {
  return compact_theta_sketch_alloc<A>(*this, ordered);
}

template<typename A>
void update_theta_sketch_alloc<A>::internal_update(uint64_t hash) {
  this->is_empty_ = false;
  if (hash >= this->theta_ or hash == 0) return; // hash == 0 is reserved to mark empty slots in the table
  if (hash_search_or_insert(hash, keys_, lg_cur_size_)) {
    num_keys_++;
    if (num_keys_ > capacity_) {
      if (lg_cur_size_ <= lg_nom_size_) {
        resize();
      } else {
        rebuild();
      }
    }
  }
}

template<typename A>
void update_theta_sketch_alloc<A>::resize() {
  const uint32_t cur_size = 1 << lg_cur_size_;
  const uint8_t lg_tgt_size = lg_nom_size_ + 1;
  const uint8_t factor = std::max(1, std::min(static_cast<int>(rf_), lg_tgt_size - lg_cur_size_));
  const uint8_t lg_new_size = lg_cur_size_ + factor;
  const uint32_t new_size = 1 << lg_new_size;
  uint64_t* new_keys = AllocU64().allocate(new_size);
  std::fill(new_keys, &new_keys[new_size], 0);
  for (uint32_t i = 0; i < cur_size; i++) {
    if (keys_[i] != 0) {
      hash_search_or_insert(keys_[i], new_keys, lg_new_size); // TODO hash_insert
    }
  }
  AllocU64().deallocate(keys_, cur_size);
  keys_ = new_keys;
  lg_cur_size_ += factor;
  capacity_ = get_capacity(lg_cur_size_, lg_nom_size_);
}

template<typename A>
void update_theta_sketch_alloc<A>::rebuild() {
  const uint32_t cur_size = 1 << lg_cur_size_;
  const uint32_t pivot = (1 << lg_nom_size_) + cur_size - num_keys_;
  std::nth_element(&keys_[0], &keys_[pivot], &keys_[cur_size]);
  this->theta_ = keys_[pivot];
  uint64_t* new_keys = AllocU64().allocate(cur_size);
  std::fill(new_keys, &new_keys[cur_size], 0);
  num_keys_ = 0;
  for (uint32_t i = 0; i < cur_size; i++) {
    if (keys_[i] != 0 and keys_[i] < this->theta_) {
      hash_search_or_insert(keys_[i], new_keys, lg_cur_size_); // TODO hash_insert
      num_keys_++;
    }
  }
  AllocU64().deallocate(keys_, cur_size);
  keys_ = new_keys;
}

template<typename A>
uint32_t update_theta_sketch_alloc<A>::get_capacity(uint8_t lg_cur_size, uint8_t lg_nom_size) {
  const double fraction = (lg_cur_size <= lg_nom_size) ? RESIZE_THRESHOLD : REBUILD_THRESHOLD;
  return std::floor(fraction * (1 << lg_cur_size));
}

template<typename A>
uint32_t update_theta_sketch_alloc<A>::get_stride(uint64_t hash, uint8_t lg_size) {
  // odd and independent of index assuming lg_size lowest bits of the hash were used for the index
  return (2 * static_cast<uint32_t>((hash >> lg_size) & STRIDE_MASK)) + 1;
}

template<typename A>
bool update_theta_sketch_alloc<A>::hash_search_or_insert(uint64_t hash, uint64_t* table, uint8_t lg_size) {
  const uint32_t mask = (1 << lg_size) - 1;
  const uint32_t stride = get_stride(hash, lg_size);
  uint32_t cur_probe = static_cast<uint32_t>(hash) & mask;

  // search for duplicate or zero
  const uint32_t loop_index = cur_probe;
  do {
    const uint64_t value = table[cur_probe];
    if (value == 0) {
      table[cur_probe] = hash; // insert value
      return true;
    } else if (value == hash) {
      return false; // found a duplicate
    }
    cur_probe = (cur_probe + stride) & mask;
  } while (cur_probe != loop_index);
  //std::cerr << "hash_search_or_insert: lg=" << (int)lg_size << ", hash=" << hash << std::endl;
  //std::cerr << "cur_probe=" << cur_probe << ", stride=" << stride << std::endl;
  throw std::logic_error("key not found and no empty slots!");
}

template<typename A>
bool update_theta_sketch_alloc<A>::hash_search(uint64_t hash, const uint64_t* table, uint8_t lg_size) {
  const uint32_t mask = (1 << lg_size) - 1;
  const uint32_t stride = update_theta_sketch_alloc<A>::get_stride(hash, lg_size);
  uint32_t cur_probe = static_cast<uint32_t>(hash) & mask;
  const uint32_t loop_index = cur_probe;
  do {
    const uint64_t value = table[cur_probe];
    if (value == 0) {
      return false;
    } else if (value == hash) {
      return true;
    }
    cur_probe = (cur_probe + stride) & mask;
  } while (cur_probe != loop_index);
  //std::cerr << "hash_search: lg=" << (int)lg_size << ", hash=" << hash << std::endl;
  //std::cerr << "cur_probe=" << cur_probe << ", stride=" << stride << std::endl;
  throw std::logic_error("key not found and search wrapped");
}

template<typename A>
typename theta_sketch_alloc<A>::const_iterator update_theta_sketch_alloc<A>::begin() const {
  return typename theta_sketch_alloc<A>::const_iterator(keys_, 1 << lg_cur_size_, 0);
}

template<typename A>
typename theta_sketch_alloc<A>::const_iterator update_theta_sketch_alloc<A>::end() const {
  return typename theta_sketch_alloc<A>::const_iterator(keys_, 1 << lg_cur_size_, 1 << lg_cur_size_);
}

// compact sketch

template<typename A>
compact_theta_sketch_alloc<A>::compact_theta_sketch_alloc(bool is_empty, uint64_t theta, uint64_t* keys, uint32_t num_keys, uint16_t seed_hash, bool is_ordered):
theta_sketch_alloc<A>(is_empty, theta),
keys_(keys),
num_keys_(num_keys),
seed_hash_(seed_hash),
is_ordered_(is_ordered)
{}

template<typename A>
compact_theta_sketch_alloc<A>::compact_theta_sketch_alloc(const compact_theta_sketch_alloc<A>& other):
theta_sketch_alloc<A>(other),
keys_(AllocU64().allocate(other.num_keys_)),
num_keys_(other.num_keys_),
seed_hash_(other.seed_hash_),
is_ordered_(other.is_ordered_)
{
  std::copy(other.keys_, &other.keys_[num_keys_], keys_);
}

template<typename A>
compact_theta_sketch_alloc<A>::compact_theta_sketch_alloc(const theta_sketch_alloc<A>& other, bool ordered):
theta_sketch_alloc<A>(other),
keys_(AllocU64().allocate(other.get_num_retained())),
num_keys_(other.get_num_retained()),
seed_hash_(other.get_seed_hash()),
is_ordered_(other.is_ordered() or ordered)
{
  std::copy(other.begin(), other.end(), keys_);
  if (ordered and !other.is_ordered()) std::sort(keys_, &keys_[num_keys_]);
}

template<typename A>
compact_theta_sketch_alloc<A>::compact_theta_sketch_alloc(compact_theta_sketch_alloc<A>&& other) noexcept:
theta_sketch_alloc<A>(std::move(other)),
keys_(nullptr),
num_keys_(other.num_keys_),
seed_hash_(other.seed_hash_),
is_ordered_(other.is_ordered_)
{
  std::swap(keys_, other.keys_);
}

template<typename A>
compact_theta_sketch_alloc<A>::~compact_theta_sketch_alloc() {
  typedef typename std::allocator_traits<A>::template rebind_alloc<uint64_t> AllocU64;
  AllocU64().deallocate(keys_, num_keys_);
}

template<typename A>
compact_theta_sketch_alloc<A>& compact_theta_sketch_alloc<A>::operator=(const compact_theta_sketch_alloc<A>& other) {
  theta_sketch_alloc<A>::operator=(other);
  if (num_keys_ != other.num_keys_) {
    AllocU64().deallocate(keys_, num_keys_);
    num_keys_ = other.num_keys_;
    keys_ = AllocU64().allocate(num_keys_);
  }
  seed_hash_ = other.seed_hash_;
  is_ordered_ = other.is_ordered_;
  std::copy(other.keys_, &other.keys_[num_keys_], keys_);
  return *this;
}

template<typename A>
compact_theta_sketch_alloc<A>& compact_theta_sketch_alloc<A>::operator=(compact_theta_sketch_alloc<A>&& other) {
  theta_sketch_alloc<A>::operator=(std::move(other));
  std::swap(keys_, other.keys_);
  std::swap(num_keys_, other.num_keys_);
  std::swap(seed_hash_, other.seed_hash_);
  std::swap(is_ordered_, other.is_ordered_);
  return *this;
}

template<typename A>
uint32_t compact_theta_sketch_alloc<A>::get_num_retained() const {
  return num_keys_;
}

template<typename A>
uint16_t compact_theta_sketch_alloc<A>::get_seed_hash() const {
  return seed_hash_;
}

template<typename A>
bool compact_theta_sketch_alloc<A>::is_ordered() const {
  return is_ordered_;
}

template<typename A>
void compact_theta_sketch_alloc<A>::to_stream(std::ostream& os, bool print_items) const {
  os << "### Compact Theta sketch summary:" << std::endl;
  os << "   num retained keys    : " << num_keys_ << std::endl;
  os << "   seed hash            : " << this->get_seed_hash() << std::endl;
  os << "   ordered?             : " << (this->is_ordered() ? "true" : "false") << std::endl;
  os << "   theta (fraction)     : " << this->get_theta() << std::endl;
  os << "   theta (raw 64-bit)   : " << this->theta_ << std::endl;
  os << "   estimation mode?     : " << (this->is_estimation_mode() ? "true" : "false") << std::endl;
  os << "   estimate             : " << this->get_estimate() << std::endl;
  os << "   lower bound 95% conf : " << this->get_lower_bound(2) << std::endl;
  os << "   upper bound 95% conf : " << this->get_upper_bound(2) << std::endl;
  os << "### End sketch summary" << std::endl;
  if (print_items) {
    os << "### Retained keys" << std::endl;
    for (auto key: *this) os << "   " << key << std::endl;
    os << "### End retained keys" << std::endl;
  }
}

template<typename A>
void compact_theta_sketch_alloc<A>::serialize(std::ostream& os) const {
  const bool is_single_item = num_keys_ == 1 and !this->is_estimation_mode();
  const uint8_t preamble_longs = this->is_empty() or is_single_item ? 1 : this->is_estimation_mode() ? 3 : 2;
  os.write((char*)&preamble_longs, sizeof(preamble_longs));
  const uint8_t serial_version = theta_sketch_alloc<A>::SERIAL_VERSION;
  os.write((char*)&serial_version, sizeof(serial_version));
  const uint8_t type = SKETCH_TYPE;
  os.write((char*)&type, sizeof(type));
  const uint16_t unused16 = 0;
  os.write((char*)&unused16, sizeof(unused16));
  const uint8_t flags_byte(
    (1 << theta_sketch_alloc<A>::flags::IS_COMPACT) |
    (1 << theta_sketch_alloc<A>::flags::IS_READ_ONLY) |
    (this->is_empty() ? 1 << theta_sketch_alloc<A>::flags::IS_EMPTY : 0) |
    (this->is_ordered() ? 1 << theta_sketch_alloc<A>::flags::IS_ORDERED : 0)
  );
  os.write((char*)&flags_byte, sizeof(flags_byte));
  const uint16_t seed_hash = get_seed_hash();
  os.write((char*)&seed_hash, sizeof(seed_hash));
  if (!this->is_empty()) {
    if (!is_single_item) {
      os.write((char*)&num_keys_, sizeof(num_keys_));
      const uint32_t unused32 = 0;
      os.write((char*)&unused32, sizeof(unused32));
      if (this->is_estimation_mode()) {
        os.write((char*)&(this->theta_), sizeof(uint64_t));
      }
    }
    os.write((char*)keys_, sizeof(uint64_t) * num_keys_);
  }
}

template<typename A>
std::pair<void_ptr_with_deleter, const size_t> compact_theta_sketch_alloc<A>::serialize(unsigned header_size_bytes) const {
  const bool is_single_item = num_keys_ == 1 and !this->is_estimation_mode();
  const uint8_t preamble_longs = this->is_empty() or is_single_item ? 1 : this->is_estimation_mode() ? 3 : 2;
  const size_t size = header_size_bytes + sizeof(uint64_t) * preamble_longs + sizeof(uint64_t) * num_keys_;
  typedef typename std::allocator_traits<A>::template rebind_alloc<char> AllocChar;
  void_ptr_with_deleter data_ptr(
    static_cast<void*>(AllocChar().allocate(size)),
    [size](void* ptr) { AllocChar().deallocate(static_cast<char*>(ptr), size); }
  );
  char* ptr = static_cast<char*>(data_ptr.get()) + header_size_bytes;

  copy_to_mem(&preamble_longs, &ptr, sizeof(preamble_longs));
  const uint8_t serial_version = theta_sketch_alloc<A>::SERIAL_VERSION;
  copy_to_mem(&serial_version, &ptr, sizeof(serial_version));
  const uint8_t type = SKETCH_TYPE;
  copy_to_mem(&type, &ptr, sizeof(type));
  const uint16_t unused16 = 0;
  copy_to_mem(&unused16, &ptr, sizeof(unused16));
  const uint8_t flags_byte(
    (1 << theta_sketch_alloc<A>::flags::IS_COMPACT) |
    (1 << theta_sketch_alloc<A>::flags::IS_READ_ONLY) |
    (this->is_empty() ? 1 << theta_sketch_alloc<A>::flags::IS_EMPTY : 0) |
    (this->is_ordered() ? 1 << theta_sketch_alloc<A>::flags::IS_ORDERED : 0)
  );
  copy_to_mem(&flags_byte, &ptr, sizeof(flags_byte));
  const uint16_t seed_hash = get_seed_hash();
  copy_to_mem(&seed_hash, &ptr, sizeof(seed_hash));
  if (!this->is_empty()) {
    if (!is_single_item) {
      copy_to_mem(&num_keys_, &ptr, sizeof(num_keys_));
      const uint32_t unused32 = 0;
      copy_to_mem(&unused32, &ptr, sizeof(unused32));
      if (this->is_estimation_mode()) {
        copy_to_mem(&(this->theta_), &ptr, sizeof(uint64_t));
      }
    }
    copy_to_mem(keys_, &ptr, sizeof(uint64_t) * num_keys_);
  }

  return std::make_pair(std::move(data_ptr), size);;
}

template<typename A>
compact_theta_sketch_alloc<A> compact_theta_sketch_alloc<A>::deserialize(std::istream& is, uint64_t seed) {
  uint8_t preamble_longs;
  is.read((char*)&preamble_longs, sizeof(preamble_longs));
  uint8_t serial_version;
  is.read((char*)&serial_version, sizeof(serial_version));
  uint8_t type;
  is.read((char*)&type, sizeof(type));
  uint16_t unused16;
  is.read((char*)&unused16, sizeof(unused16));
  uint8_t flags_byte;
  is.read((char*)&flags_byte, sizeof(flags_byte));
  uint16_t seed_hash;
  is.read((char*)&seed_hash, sizeof(seed_hash));
  theta_sketch_alloc<A>::check_sketch_type(type, SKETCH_TYPE);
  theta_sketch_alloc<A>::check_serial_version(serial_version, theta_sketch_alloc<A>::SERIAL_VERSION);
  theta_sketch_alloc<A>::check_seed_hash(seed_hash, theta_sketch_alloc<A>::get_seed_hash(seed));
  return internal_deserialize(is, preamble_longs, flags_byte, seed_hash);
}

template<typename A>
compact_theta_sketch_alloc<A> compact_theta_sketch_alloc<A>::internal_deserialize(std::istream& is, uint8_t preamble_longs, uint8_t flags_byte, uint16_t seed_hash) {
  uint64_t theta = theta_sketch_alloc<A>::MAX_THETA;
  uint64_t* keys = nullptr;
  uint32_t num_keys = 0;

  const bool is_empty = flags_byte & (1 << theta_sketch_alloc<A>::flags::IS_EMPTY);
  if (!is_empty) {
    if (preamble_longs == 1) {
      num_keys = 1;
    } else {
      is.read((char*)&num_keys, sizeof(num_keys));
      uint32_t unused32;
      is.read((char*)&unused32, sizeof(unused32));
      if (preamble_longs > 2) {
        is.read((char*)&theta, sizeof(theta));
      }
    }
    typedef typename std::allocator_traits<A>::template rebind_alloc<uint64_t> AllocU64;
    keys = AllocU64().allocate(num_keys);
    is.read((char*)keys, sizeof(uint64_t) * num_keys);
  }

  const bool is_ordered = flags_byte & (1 << theta_sketch_alloc<A>::flags::IS_ORDERED);
  return compact_theta_sketch_alloc<A>(is_empty, theta, keys, num_keys, seed_hash, is_ordered);
}

template<typename A>
compact_theta_sketch_alloc<A> compact_theta_sketch_alloc<A>::deserialize(const void* bytes, size_t size, uint64_t seed) {
  theta_sketch_alloc<A>::check_size(size, 8);
  const char* ptr = static_cast<const char*>(bytes);
  uint8_t preamble_longs;
  copy_from_mem(&ptr, &preamble_longs, sizeof(preamble_longs));
  uint8_t serial_version;
  copy_from_mem(&ptr, &serial_version, sizeof(serial_version));
  uint8_t type;
  copy_from_mem(&ptr, &type, sizeof(type));
  uint16_t unused16;
  copy_from_mem(&ptr, &unused16, sizeof(unused16));
  uint8_t flags_byte;
  copy_from_mem(&ptr, &flags_byte, sizeof(flags_byte));
  uint16_t seed_hash;
  copy_from_mem(&ptr, &seed_hash, sizeof(seed_hash));
  theta_sketch_alloc<A>::check_sketch_type(type, SKETCH_TYPE);
  theta_sketch_alloc<A>::check_serial_version(serial_version, theta_sketch_alloc<A>::SERIAL_VERSION);
  theta_sketch_alloc<A>::check_seed_hash(seed_hash, theta_sketch_alloc<A>::get_seed_hash(seed));
  return internal_deserialize(ptr, size - (ptr - static_cast<const char*>(bytes)), preamble_longs, flags_byte, seed_hash);
}

template<typename A>
compact_theta_sketch_alloc<A> compact_theta_sketch_alloc<A>::internal_deserialize(const void* bytes, size_t size, uint8_t preamble_longs, uint8_t flags_byte, uint16_t seed_hash) {
  const char* ptr = static_cast<const char*>(bytes);

  uint64_t theta = theta_sketch_alloc<A>::MAX_THETA;
  uint64_t* keys = nullptr;
  uint32_t num_keys = 0;

  const bool is_empty = flags_byte & (1 << theta_sketch_alloc<A>::flags::IS_EMPTY);
  if (!is_empty) {
    if (preamble_longs == 1) {
      num_keys = 1;
    } else {
      theta_sketch_alloc<A>::check_size(size, 8);
      copy_from_mem(&ptr, &num_keys, sizeof(num_keys));
      uint32_t unused32;
      copy_from_mem(&ptr, &unused32, sizeof(unused32));
      if (preamble_longs > 2) {
        theta_sketch_alloc<A>::check_size(size - (ptr - static_cast<const char*>(bytes)), 8);
        copy_from_mem(&ptr, &theta, sizeof(theta));
      }
    }
    const size_t keys_size_bytes = sizeof(uint64_t) * num_keys;
    theta_sketch_alloc<A>::check_size(size - (ptr - static_cast<const char*>(bytes)), keys_size_bytes);
    typedef typename std::allocator_traits<A>::template rebind_alloc<uint64_t> AllocU64;
    keys = AllocU64().allocate(num_keys);
    copy_from_mem(&ptr, keys, keys_size_bytes);
  }

  const bool is_ordered = flags_byte & (1 << theta_sketch_alloc<A>::flags::IS_ORDERED);
  return compact_theta_sketch_alloc<A>(is_empty, theta, keys, num_keys, seed_hash, is_ordered);
}

template<typename A>
typename theta_sketch_alloc<A>::const_iterator compact_theta_sketch_alloc<A>::begin() const {
  return typename theta_sketch_alloc<A>::const_iterator(keys_, num_keys_, 0);
}

template<typename A>
typename theta_sketch_alloc<A>::const_iterator compact_theta_sketch_alloc<A>::end() const {
  return typename theta_sketch_alloc<A>::const_iterator(keys_, num_keys_, num_keys_);
}

// builder

template<typename A>
update_theta_sketch_alloc<A>::builder::builder():
lg_k_(DEFAULT_LG_K), rf_(DEFAULT_RESIZE_FACTOR), p_(1), seed_(DEFAULT_SEED) {}

template<typename A>
typename update_theta_sketch_alloc<A>::builder& update_theta_sketch_alloc<A>::builder::set_lg_k(uint8_t lg_k) {
  if (lg_k < MIN_LG_K) {
    throw std::invalid_argument("lg_k must not be less than " + std::to_string(MIN_LG_K) + ": " + std::to_string(lg_k));
  }
  lg_k_ = lg_k;
  return *this;
}

template<typename A>
typename update_theta_sketch_alloc<A>::builder& update_theta_sketch_alloc<A>::builder::set_resize_factor(resize_factor rf) {
  rf_ = rf;
  return *this;
}

template<typename A>
typename update_theta_sketch_alloc<A>::builder& update_theta_sketch_alloc<A>::builder::set_p(float p) {
  p_ = p;
  return *this;
}

template<typename A>
typename update_theta_sketch_alloc<A>::builder& update_theta_sketch_alloc<A>::builder::set_seed(uint64_t seed) {
  seed_ = seed;
  return *this;
}

template<typename A>
uint8_t update_theta_sketch_alloc<A>::builder::starting_sub_multiple(uint8_t lg_tgt, uint8_t lg_min, uint8_t lg_rf) {
  return (lg_tgt <= lg_min) ? lg_min : (lg_rf == 0) ? lg_tgt : ((lg_tgt - lg_min) % lg_rf) + lg_min;
}

template<typename A>
update_theta_sketch_alloc<A> update_theta_sketch_alloc<A>::builder::build() const {
  return update_theta_sketch_alloc<A>(starting_sub_multiple(lg_k_ + 1, MIN_LG_K, static_cast<uint8_t>(rf_)), lg_k_, rf_, p_, seed_);
}

// iterator

template<typename A>
theta_sketch_alloc<A>::const_iterator::const_iterator(const uint64_t* keys, uint32_t size, uint32_t index):
keys_(keys), size_(size), index_(index) {
  while (index_ < size_ and keys_[index_] == 0) ++index_;
}

template<typename A>
typename theta_sketch_alloc<A>::const_iterator& theta_sketch_alloc<A>::const_iterator::operator++() {
  do {
    ++index_;
  } while (index_ < size_ and keys_[index_] == 0);
  return *this;
}

template<typename A>
typename theta_sketch_alloc<A>::const_iterator theta_sketch_alloc<A>::const_iterator::operator++(int) {
  const_iterator tmp(*this);
  operator++();
  return tmp;
}

template<typename A>
bool theta_sketch_alloc<A>::const_iterator::operator==(const const_iterator& other) const {
  return index_ == other.index_;
}

template<typename A>
bool theta_sketch_alloc<A>::const_iterator::operator!=(const const_iterator& other) const {
  return index_ != other.index_;
}

template<typename A>
uint64_t theta_sketch_alloc<A>::const_iterator::operator*() const {
  return keys_[index_];
}

} /* namespace datasketches */

# endif
