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

#ifndef _BLOOM_FILTER_IMPL_HPP_
#define _BLOOM_FILTER_IMPL_HPP_

#include <memory>
#include <sstream>
#include <vector>

#include "common_defs.hpp"
#include "bit_array_ops.hpp"
#include "memory_operations.hpp"
#include "xxhash64.h"

// memory scenarios:
// * on-heap: owned, bit_array_ set, memory_ null
// * direct: not owned, bit_array_ set, memory_ set
//   * read-only an option for direct

namespace datasketches {

template<typename A>
bloom_filter_alloc<A>::bloom_filter_alloc(uint64_t num_bits, uint16_t num_hashes, uint64_t seed, const A& allocator) :
  allocator_(allocator),
  seed_(seed),
  num_hashes_(num_hashes),
  is_dirty_(false),
  is_owned_(true),
  is_read_only_(false),
  capacity_bits_((num_bits + 63) & ~0x3F), // can round to nearest multiple of 64 prior to bounds checks
  num_bits_set_(0)
{
  if (num_hashes == 0) {
    throw std::invalid_argument("Must have at least 1 hash function");
  }
  if (num_bits == 0) {
    throw std::invalid_argument("Number of bits must be greater than zero");
  } else if (num_bits > MAX_FILTER_SIZE_BITS) {
    throw std::invalid_argument("Filter may not exceed " + std::to_string(MAX_FILTER_SIZE_BITS) + " bits");
  }

  const uint64_t num_bytes = capacity_bits_ >> 3;
  bit_array_ = AllocUint8(allocator_).allocate(num_bytes);
  std::fill_n(bit_array_, num_bytes, 0);
  if (bit_array_ == nullptr) {
    throw std::bad_alloc();
  }
  memory_ = nullptr;
}

template<typename A>
bloom_filter_alloc<A>::bloom_filter_alloc(uint8_t* memory,
                                          size_t length_bytes,
                                          uint64_t num_bits,
                                          uint16_t num_hashes,
                                          uint64_t seed,
                                          const A& allocator) :
  allocator_(allocator),
  seed_(seed),
  num_hashes_(num_hashes),
  is_dirty_(false),
  is_owned_(false),
  is_read_only_(false),
  capacity_bits_((num_bits + 63) & ~0x3F), // can round to nearest multiple of 64 prior to bounds checks
  num_bits_set_(0)
{
  if (num_hashes == 0) {
    throw std::invalid_argument("Must have at least 1 hash function");
  }
  if (num_bits == 0) {
    throw std::invalid_argument("Number of bits must be greater than zero");
  } else if (num_bits > MAX_FILTER_SIZE_BITS) {
    throw std::invalid_argument("Filter may not exceed " + std::to_string(MAX_FILTER_SIZE_BITS) + " bits");
  }

  const size_t num_bytes = get_serialized_size_bytes(capacity_bits_);
  if (length_bytes < num_bytes) {
    throw std::invalid_argument("Input memory block is too small");
  }

  // fill in header info
  uint8_t* ptr = memory;
  const uint8_t preamble_longs = PREAMBLE_LONGS_STANDARD; // no resizing so assume non-empty
  ptr += copy_to_mem(preamble_longs, ptr);
  const uint8_t serial_version = SER_VER;
  ptr += copy_to_mem(serial_version, ptr);
  const uint8_t family = FAMILY_ID;
  ptr += copy_to_mem(family, ptr);
  const uint8_t flags_byte = 0; // again, assuming non-empty
  ptr += copy_to_mem(flags_byte, ptr);

  ptr += copy_to_mem(num_hashes_, ptr);
  ptr += copy_to_mem(static_cast<uint16_t>(0), ptr); // 2 bytes unused
  ptr += copy_to_mem(seed_, ptr);
  ptr += copy_to_mem(static_cast<int32_t>(capacity_bits_ >> 6), ptr); // sized in java longs
  ptr += copy_to_mem(static_cast<uint32_t>(0), ptr); // 4 bytes unused

  // rest of memory is num bits and bit array, so start with zeroes
  std::fill_n(ptr, sizeof(uint64_t) * ((capacity_bits_ >> 6) + 1), 0);
  bit_array_ = memory + BIT_ARRAY_OFFSET_BYTES;
  memory_ = memory;
}

template<typename A>
bloom_filter_alloc<A>::bloom_filter_alloc(uint64_t seed,
                                          uint16_t num_hashes,
                                          bool is_dirty,
                                          bool is_owned,
                                          bool is_read_only,
                                          uint64_t capacity_bits,
                                          uint64_t num_bits_set,
                                          uint8_t* bit_array,
                                          uint8_t* memory,
                                          const A& allocator) :
  allocator_(allocator),
  seed_(seed),
  num_hashes_(num_hashes),
  is_dirty_(is_dirty),
  is_owned_(is_owned),
  is_read_only_(is_read_only),
  capacity_bits_((capacity_bits + 63) & ~0x3F),
  num_bits_set_(num_bits_set),
  bit_array_(bit_array),
  memory_(memory)
{
  // private constructor
  // no consistency checks since we should have done those prior to calling this
  if (is_read_only_ && memory_ != nullptr && num_bits_set == DIRTY_BITS_VALUE) {
    num_bits_set_ = bit_array_ops::count_num_bits_set(bit_array_, capacity_bits_ >> 3);
  }
}

template<typename A>
bloom_filter_alloc<A>::bloom_filter_alloc(const bloom_filter_alloc& other) :
  allocator_(other.allocator_),
  seed_(other.seed_),
  num_hashes_(other.num_hashes_),
  is_dirty_(other.is_dirty_),
  is_owned_(other.is_owned_),
  is_read_only_(other.is_read_only_),
  capacity_bits_(other.capacity_bits_),
  num_bits_set_(other.num_bits_set_)
{
  if (is_owned_) {
    const size_t num_bytes = capacity_bits_ >> 3;
    bit_array_ = AllocUint8(allocator_).allocate(num_bytes);
    if (bit_array_ == nullptr) {
      throw std::bad_alloc();
    }
    std::copy_n(other.bit_array_, num_bytes, bit_array_);
    memory_ = nullptr;
  } else {
    bit_array_ = other.bit_array_;
    memory_ = other.memory_;
  }
}

template<typename A>
bloom_filter_alloc<A>::bloom_filter_alloc(bloom_filter_alloc&& other) noexcept :
  allocator_(std::move(other.allocator_)),
  seed_(other.seed_),
  num_hashes_(other.num_hashes_),
  is_dirty_(other.is_dirty_),
  is_owned_(other.is_owned_),
  is_read_only_(other.is_read_only_),
  capacity_bits_(other.capacity_bits_),
  num_bits_set_(other.num_bits_set_),
  bit_array_(std::move(other.bit_array_)),
  memory_(std::move(other.memory_))
{
  // ensure destructor on other will behave nicely
  other.is_owned_ = false;
  other.bit_array_ = nullptr;
  other.memory_ = nullptr;
}

template<typename A>
bloom_filter_alloc<A>& bloom_filter_alloc<A>::operator=(const bloom_filter_alloc& other) {
  bloom_filter_alloc<A> copy(other);
  std::swap(allocator_, copy.allocator_);
  std::swap(seed_, copy.seed_);
  std::swap(num_hashes_, copy.num_hashes_);
  std::swap(is_dirty_, copy.is_dirty_);
  std::swap(is_owned_, copy.is_owned_);
  std::swap(is_read_only_, copy.is_read_only_);
  std::swap(capacity_bits_, copy.capacity_bits_);
  std::swap(num_bits_set_, copy.num_bits_set_);
  std::swap(bit_array_, copy.bit_array_);
  std::swap(memory_, copy.memory_);
  return *this;
}

template<typename A>
bloom_filter_alloc<A>& bloom_filter_alloc<A>::operator=(bloom_filter_alloc&& other) {
  if (this == &other) { return *this; }
  std::swap(allocator_, other.allocator_);
  std::swap(seed_, other.seed_);
  std::swap(num_hashes_, other.num_hashes_);
  std::swap(is_dirty_, other.is_dirty_);
  std::swap(is_owned_, other.is_owned_);
  std::swap(is_read_only_, other.is_read_only_);
  std::swap(capacity_bits_, other.capacity_bits_);
  std::swap(num_bits_set_, other.num_bits_set_);
  std::swap(bit_array_, other.bit_array_);
  std::swap(memory_, other.memory_);
  return *this;
}

template<typename A>
bloom_filter_alloc<A>::~bloom_filter_alloc() {
  if (is_owned_) {
    if (memory_ != nullptr) {
      // deallocate total memory_ block, including preamble
      AllocUint8(allocator_).deallocate(memory_, (capacity_bits_ >> 3) + BIT_ARRAY_OFFSET_BYTES);
    } else if (bit_array_ != nullptr) {
      // only need to deallocate bit_array_
      AllocUint8(allocator_).deallocate(bit_array_, capacity_bits_ >> 3);
    }
    memory_ = nullptr;
    bit_array_ = nullptr;
  }
}

template<typename A>
bloom_filter_alloc<A> bloom_filter_alloc<A>::deserialize(const void* bytes, size_t length_bytes, const A& allocator) {
  // not wrapping so we can cast away const as we're not modifying the memory
  return internal_deserialize_or_wrap(const_cast<void*>(bytes), length_bytes, false, false, allocator);
}

/*
 * A Bloom Filter's serialized image always uses 3 longs of preamble when empty,
 * otherwise 4 longs:
 *
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||       0        |    1   |    2   |    3   |    4   |    5   |    6   |    7   |
 *  0   || Preamble_Longs | SerVer | FamID  |  Flags |----Num Hashes---|-----Unused------|
 *
 *      ||       8        |    9   |   10   |   11   |   12   |   13   |   14   |   15   |
 *  1   ||---------------------------------Hash Seed-------------------------------------|
 *
 *      ||      16        |   17   |   18   |   19   |   20   |   21   |   22   |   23   |
 *  2   ||-------BitArray Length (in longs)----------|-----------Unused------------------|
 *
 *      ||      24        |   25   |   26   |   27   |   28   |   29   |   30   |   31   |
 *  3   ||---------------------------------NumBitsSet------------------------------------|
 *  </pre>
 *
 * The raw BitArray bits, if non-empty start at byte 32.
 */

template<typename A>
bloom_filter_alloc<A> bloom_filter_alloc<A>::deserialize(std::istream& is, const A& allocator) {
  const uint8_t prelongs = read<uint8_t>(is);
  const uint8_t ser_ver = read<uint8_t>(is);
  const uint8_t family = read<uint8_t>(is);
  const uint8_t flags = read<uint8_t>(is);

  if (prelongs < 1 || prelongs > 4) {
    throw std::invalid_argument("Possible corruption: Incorrect number of preamble bytes specified in header");
  }
  if (ser_ver != SER_VER) {
    throw std::invalid_argument("Possible corruption: Unrecognized serialization version: " + std::to_string(ser_ver));
  }
  if (family != FAMILY_ID) {
    throw std::invalid_argument("Possible corruption: Incorrect Family ID for bloom filter. Found: " + std::to_string(family));
  }

  const bool is_empty = (flags & EMPTY_FLAG_MASK) != 0;

  const uint16_t num_hashes = read<uint16_t>(is);
  read<uint16_t>(is); // unused
  const uint64_t seed = read<uint64_t>(is);
  const uint32_t num_longs = read<uint32_t>(is); // sized in java longs
  read<uint32_t>(is); // unused

  // if empty, stop reading
  if (is_empty) {
    return bloom_filter_alloc<A>(num_longs << 6, num_hashes, seed, allocator);
  }

  const uint64_t num_bits_set = read<uint64_t>(is);
  const bool is_dirty = (num_bits_set == DIRTY_BITS_VALUE);

  // allocate memory
  const uint64_t num_bytes = num_longs << 3;
  AllocUint8 alloc(allocator);
  uint8_t* bit_array = alloc.allocate(num_bytes);
  if (bit_array == nullptr) {
    throw std::bad_alloc();
  }
  read(is, bit_array, num_bytes);

  // pass to constructor
  return bloom_filter_alloc<A>(seed, num_hashes, is_dirty, true, false, num_longs << 6, num_bits_set, bit_array, nullptr, allocator);
}

template<typename A>
const bloom_filter_alloc<A> bloom_filter_alloc<A>::wrap(const void* bytes, size_t length_bytes, const A& allocator) {
  // read-only flag means we won't modify the memory, but cast away the const
  return internal_deserialize_or_wrap(const_cast<void*>(bytes), length_bytes, true, true, allocator);
}

template<typename A>
bloom_filter_alloc<A> bloom_filter_alloc<A>::writable_wrap(void* bytes, size_t length_bytes, const A& allocator) {
  return internal_deserialize_or_wrap(bytes, length_bytes, false, true, allocator);
}

template<typename A>
bloom_filter_alloc<A> bloom_filter_alloc<A>::internal_deserialize_or_wrap(void* bytes,
                                                                          size_t length_bytes,
                                                                          bool read_only,
                                                                          bool wrap,
                                                                          const A& allocator)
{
  ensure_minimum_memory(length_bytes, 8);
  if (bytes == nullptr) {
    throw std::invalid_argument("Input data is null or empty");
  }
  const uint8_t* ptr = static_cast<const uint8_t*>(bytes);
  const uint8_t* end_ptr = ptr + length_bytes;
  const uint8_t prelongs = *ptr++;
  const uint8_t ser_ver = *ptr++;
  const uint8_t family = *ptr++;
  const uint8_t flags = *ptr++;

  if (prelongs < PREAMBLE_LONGS_EMPTY || prelongs > PREAMBLE_LONGS_STANDARD) {
    throw std::invalid_argument("Possible corruption: Incorrect number of preamble bytes specified in header");
  }
  if (ser_ver != SER_VER) {
    throw std::invalid_argument("Possible corruption: Unrecognized serialization version: " + std::to_string(ser_ver));
  }
  if (family != FAMILY_ID) {
    throw std::invalid_argument("Possible corruption: Incorrect Family ID for bloom filter. Found: " + std::to_string(family));
  }

  const bool is_empty = (flags & EMPTY_FLAG_MASK) != 0;

  ensure_minimum_memory(length_bytes, prelongs * sizeof(uint64_t));

  uint16_t num_hashes;
  ptr += copy_from_mem(ptr, num_hashes);
  ptr += sizeof(uint16_t); // 16 bits unused after num_hashes
  uint64_t seed;
  ptr += copy_from_mem(ptr, seed);

  uint32_t num_longs;
  ptr += copy_from_mem(ptr, num_longs); // sized in java longs
  ptr += sizeof(uint32_t); // unused 32 bits follow

  // if empty, stop reading
  if (wrap && is_empty && !read_only) {
    throw std::invalid_argument("Cannot wrap an empty filter for writing");
  } else if (is_empty) {
    return bloom_filter_alloc<A>(num_longs << 6, num_hashes, seed, allocator);
  }

  uint64_t num_bits_set;
  ptr += copy_from_mem(ptr, num_bits_set);
  const bool is_dirty = (num_bits_set == DIRTY_BITS_VALUE);

  uint8_t* bit_array;
  uint8_t* memory;
  if (wrap) {
    memory = static_cast<uint8_t*>(bytes);
    bit_array = memory + BIT_ARRAY_OFFSET_BYTES;
  } else {
    // allocate memory
    memory = nullptr;
    const uint64_t num_bytes = num_longs << 3;
    ensure_minimum_memory(end_ptr - ptr, num_bytes);
    AllocUint8 alloc(allocator);
    bit_array = alloc.allocate(num_bytes);
    if (bit_array == nullptr) {
      throw std::bad_alloc();
    }
    copy_from_mem(ptr, bit_array, num_bytes);
  }

  // pass to constructor -- !wrap == is_owned_
  return bloom_filter_alloc<A>(seed, num_hashes, is_dirty, !wrap, read_only, num_longs << 6, num_bits_set, bit_array, memory, allocator);
}

template<typename A>
void bloom_filter_alloc<A>::serialize(std::ostream& os) const {
  // Should we serialize memory_ directly if it exists?
  const uint8_t preamble_longs = is_empty() ? PREAMBLE_LONGS_EMPTY : PREAMBLE_LONGS_STANDARD;
  write(os, preamble_longs);
  const uint8_t serial_version = SER_VER;
  write(os, serial_version);
  const uint8_t family = FAMILY_ID;
  write(os, family);
  const uint8_t flags_byte = is_empty() ? EMPTY_FLAG_MASK : 0;
  write(os, flags_byte);

  write(os, num_hashes_);
  write(os, static_cast<uint16_t>(0)); // 2 bytes unused
  write(os, seed_);
  write(os, static_cast<int32_t>(capacity_bits_ >> 6)); // sized in java longs
  write(os, static_cast<uint32_t>(0)); // 4 bytes unused

  if (!is_empty()) {
    write(os, is_dirty_ ? DIRTY_BITS_VALUE : num_bits_set_);
    write(os, bit_array_, capacity_bits_ >> 3);
  }

  os.flush();
}

template<typename A>
auto bloom_filter_alloc<A>::serialize(unsigned header_size_bytes) const -> vector_bytes {
  // Should we serialize memory_ directly if it exists?
  const size_t size = header_size_bytes + get_serialized_size_bytes();
  vector_bytes bytes(size, 0, allocator_);
  uint8_t* ptr = bytes.data() + header_size_bytes;

  const uint8_t preamble_longs = is_empty() ? PREAMBLE_LONGS_EMPTY : PREAMBLE_LONGS_STANDARD;
  ptr += copy_to_mem(preamble_longs, ptr);
  const uint8_t serial_version = SER_VER;
  ptr += copy_to_mem(serial_version, ptr);
  const uint8_t family = FAMILY_ID;
  ptr += copy_to_mem(family, ptr);
  const uint8_t flags_byte = is_empty() ? EMPTY_FLAG_MASK : 0;
  ptr += copy_to_mem(flags_byte, ptr);

  ptr += copy_to_mem(num_hashes_, ptr);
  ptr += copy_to_mem(static_cast<uint16_t>(0), ptr); // 2 bytes unused
  ptr += copy_to_mem(seed_, ptr);
  ptr += copy_to_mem(static_cast<int32_t>(capacity_bits_ >> 6), ptr); // sized in java longs
  ptr += copy_to_mem(static_cast<uint32_t>(0), ptr); // 4 bytes unused

  if (!is_empty()) {
    ptr += copy_to_mem(is_dirty_ ? DIRTY_BITS_VALUE : num_bits_set_, ptr);
    ptr += copy_to_mem(bit_array_, ptr, capacity_bits_ >> 3);
  }

  return bytes;
}

template<typename A>
size_t bloom_filter_alloc<A>::get_serialized_size_bytes() const {
  return sizeof(uint64_t) * (is_empty() ? PREAMBLE_LONGS_EMPTY : PREAMBLE_LONGS_STANDARD + (capacity_bits_ >> 6));
}

template<typename A>
size_t bloom_filter_alloc<A>::get_serialized_size_bytes(uint64_t num_bits) {
  if (num_bits == 0)
    throw std::invalid_argument("Number of bits must be greater than zero");

  size_t num_bytes = (num_bits + 63) >> 6;
  return sizeof(uint64_t) * (PREAMBLE_LONGS_STANDARD + num_bytes);
}

template<typename A>
bool bloom_filter_alloc<A>::is_empty() const {
  return !is_dirty_ && num_bits_set_ == 0;
}

template<typename A>
uint64_t bloom_filter_alloc<A>::get_bits_used() {
  if (is_dirty_) {
    num_bits_set_ = bit_array_ops::count_num_bits_set(bit_array_, capacity_bits_ >> 3);
    is_dirty_ = false;
  }
  return num_bits_set_;
}

template<typename A>
uint64_t bloom_filter_alloc<A>::get_capacity() const {
  return capacity_bits_;
}

template<typename A>
uint16_t bloom_filter_alloc<A>::get_num_hashes() const {
  return num_hashes_;
}

template<typename A>
uint64_t bloom_filter_alloc<A>::get_seed() const {
  return seed_;
}

template<typename A>
bool bloom_filter_alloc<A>::is_read_only() const {
  return is_read_only_;
}

template<typename A>
bool bloom_filter_alloc<A>::is_wrapped() const {
  return memory_ != nullptr;
}

template<typename A>
bool bloom_filter_alloc<A>::is_memory_owned() const {
  return is_owned_;
}

template<typename A>
const uint8_t* bloom_filter_alloc<A>::get_wrapped_memory() const {
  return memory_;
}

template<typename A>
void bloom_filter_alloc<A>::reset() {
  if (is_read_only_) {
    throw std::logic_error("Cannot reset a read-only filter");
  }
  update_num_bits_set(0);
  std::fill_n(bit_array_, capacity_bits_ >> 3, 0);
}

template<typename A>
void bloom_filter_alloc<A>::update_num_bits_set(uint64_t num_bits_set) {
  num_bits_set_ = num_bits_set;
  is_dirty_ = false;
  if (memory_ != nullptr && !is_read_only_) {
    copy_to_mem(num_bits_set_, memory_ + NUM_BITS_SET_OFFSET_BYTES);
  }
}

// UPDATE METHODS

template<typename A>
void bloom_filter_alloc<A>::update(std::string& item) {
  if (item.empty()) return;
  const uint64_t h0 = XXHash64::hash(item.data(), item.size(), seed_);
  const uint64_t h1 = XXHash64::hash(item.data(), item.size(), h0);
  internal_update(h0, h1);
}

template<typename A>
void bloom_filter_alloc<A>::update(uint64_t item) {
  const uint64_t h0 = XXHash64::hash(&item, sizeof(item), seed_);
  const uint64_t h1 = XXHash64::hash(&item, sizeof(item), h0);
  internal_update(h0, h1);
}

template<typename A>
void bloom_filter_alloc<A>::update(uint32_t item) {
  update(static_cast<uint64_t>(item));
}

template<typename A>
void bloom_filter_alloc<A>::update(uint16_t item) {
  update(static_cast<uint64_t>(item));
}

template<typename A>
void bloom_filter_alloc<A>::update(uint8_t item) {
  update(static_cast<uint64_t>(item));
}

template<typename A>
void bloom_filter_alloc<A>::update(int64_t item) {
  const uint64_t h0 = XXHash64::hash(&item, sizeof(item), seed_);
  const uint64_t h1 = XXHash64::hash(&item, sizeof(item), h0);
  internal_update(h0, h1);
}

template<typename A>
void bloom_filter_alloc<A>::update(int32_t item) {
  update(static_cast<int64_t>(item));
}

template<typename A>
void bloom_filter_alloc<A>::update(int16_t item) {
  update(static_cast<int64_t>(item));
}

template<typename A>
void bloom_filter_alloc<A>::update(int8_t item) {
  update(static_cast<int64_t>(item));
}

template<typename A>
void bloom_filter_alloc<A>::update(double item) {
  union {
    int64_t long_value;
    double double_value;
  } ldu;
  ldu.double_value = static_cast<double>(item);
  if (item == 0.0) {
    ldu.double_value = 0.0; // canonicalize -0.0 to 0.0
  } else if (std::isnan(ldu.double_value)) {
    ldu.long_value = 0x7ff8000000000000L; // canonicalize NaN using value from Java's Double.doubleToLongBits()
  }
  const uint64_t h0 = XXHash64::hash(&ldu, sizeof(ldu), seed_);
  const uint64_t h1 = XXHash64::hash(&ldu, sizeof(ldu), h0);
  internal_update(h0, h1);
}

template<typename A>
void bloom_filter_alloc<A>::update(float item) {
  update(static_cast<double>(item));
}

template<typename A>
void bloom_filter_alloc<A>::update(const void* item, size_t size) {
  if (item == nullptr || size == 0) return;
  const uint64_t h0 = XXHash64::hash(item, size, seed_);
  const uint64_t h1 = XXHash64::hash(item, size, h0);
  internal_update(h0, h1);
}

template<typename A>
void bloom_filter_alloc<A>::internal_update(uint64_t h0, uint64_t h1) {
  if (is_read_only_) {
    throw std::logic_error("Cannot update a read-only filter");
  }
  const uint64_t num_bits = get_capacity();
  for (uint16_t i = 1; i <= num_hashes_; i++) {
    const uint64_t hash_index = ((h0 + i * h1) >> 1) % num_bits;
    bit_array_ops::set_bit(bit_array_, hash_index);
  }
  is_dirty_ = true;
}

// QUERY-AND-UPDATE METHODS

template<typename A>
bool bloom_filter_alloc<A>::query_and_update(const std::string& item) {
  if (item.empty()) return false;
  const uint64_t h0 = XXHash64::hash(item.data(), item.size(), seed_);
  const uint64_t h1 = XXHash64::hash(item.data(), item.size(), h0);
  return internal_query_and_update(h0, h1);
}

template<typename A>
bool bloom_filter_alloc<A>::query_and_update(uint64_t item) {
  const uint64_t h0 = XXHash64::hash(&item, sizeof(item), seed_);
  const uint64_t h1 = XXHash64::hash(&item, sizeof(item), h0);
  return internal_query_and_update(h0, h1);
}

template<typename A>
bool bloom_filter_alloc<A>::query_and_update(uint32_t item) {
  return query_and_update(static_cast<uint64_t>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query_and_update(uint16_t item) {
  return query_and_update(static_cast<uint64_t>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query_and_update(uint8_t item) {
  return query_and_update(static_cast<uint64_t>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query_and_update(int64_t item) {
  const uint64_t h0 = XXHash64::hash(&item, sizeof(item), seed_);
  const uint64_t h1 = XXHash64::hash(&item, sizeof(item), h0);
  return internal_query_and_update(h0, h1);
}

template<typename A>
bool bloom_filter_alloc<A>::query_and_update(int32_t item) {
  return query_and_update(static_cast<int64_t>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query_and_update(int16_t item) {
  return query_and_update(static_cast<int64_t>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query_and_update(int8_t item) {
  return query_and_update(static_cast<int64_t>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query_and_update(double item) {
  union {
    int64_t long_value;
    double double_value;
  } ldu;
  ldu.double_value = item;
  if (item == 0.0) {
    ldu.double_value = 0.0; // canonicalize -0.0 to 0.0
  } else if (std::isnan(ldu.double_value)) {
    ldu.long_value = 0x7ff8000000000000L; // canonicalize NaN using value from Java's Double.doubleToLongBits()
  }
  const uint64_t h0 = XXHash64::hash(&ldu, sizeof(ldu), seed_);
  const uint64_t h1 = XXHash64::hash(&ldu, sizeof(ldu), h0);
  return internal_query_and_update(h0, h1);
}

template<typename A>
bool bloom_filter_alloc<A>::query_and_update(float item) {
  return query_and_update(static_cast<double>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query_and_update(const void* item, size_t size) {
  if (item == nullptr || size == 0) return false;
  const uint64_t h0 = XXHash64::hash(item, size, seed_);
  const uint64_t h1 = XXHash64::hash(item, size, h0);
  return internal_query_and_update(h0, h1);
}

template<typename A>
bool bloom_filter_alloc<A>::internal_query_and_update(uint64_t h0, uint64_t h1) {
  if (is_read_only_) {
    throw std::logic_error("Cannot update a read-only filter");
  }
  const uint64_t num_bits = get_capacity();
  bool value_exists = true;
  for (uint16_t i = 1; i <= num_hashes_; i++) {
    const uint64_t hash_index = ((h0 + i * h1) >> 1) % num_bits;
    bool value = bit_array_ops::get_and_set_bit(bit_array_, hash_index);
    update_num_bits_set(num_bits_set_ + (value ? 0 : 1));
    value_exists &= value;
  }
  return value_exists;
}

// QUERY METHODS

template<typename A>
bool bloom_filter_alloc<A>::query(const std::string& item) const {
  if (item.empty()) return false;
  const uint64_t h0 = XXHash64::hash(item.data(), item.size(), seed_);
  const uint64_t h1 = XXHash64::hash(item.data(), item.size(), h0);
  return internal_query(h0, h1);
}

template<typename A>
bool bloom_filter_alloc<A>::query(uint64_t item) const {
  const uint64_t h0 = XXHash64::hash(&item, sizeof(item), seed_);
  const uint64_t h1 = XXHash64::hash(&item, sizeof(item), h0);
  return internal_query(h0, h1);
}

template<typename A>
bool bloom_filter_alloc<A>::query(uint32_t item) const {
  return query(static_cast<uint64_t>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query(uint16_t item) const {
  return query(static_cast<uint64_t>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query(uint8_t item) const {
  return query(static_cast<uint64_t>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query(int64_t item) const {
  const uint64_t h0 = XXHash64::hash(&item, sizeof(item), seed_);
  const uint64_t h1 = XXHash64::hash(&item, sizeof(item), h0);
  return internal_query(h0, h1);
}

template<typename A>
bool bloom_filter_alloc<A>::query(int32_t item) const {
  return query(static_cast<int64_t>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query(int16_t item) const {
  return query(static_cast<int64_t>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query(int8_t item) const {
  return query(static_cast<int64_t>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query(double item) const {
  union {
    int64_t long_value;
    double double_value;
  } ldu;
  ldu.double_value = static_cast<double>(item);
  if (item == 0.0) {
    ldu.double_value = 0.0; // canonicalize -0.0 to 0.0
  } else if (std::isnan(ldu.double_value)) {
    ldu.long_value = 0x7ff8000000000000L; // canonicalize NaN using value from Java's Double.doubleToLongBits()
  }
  const uint64_t h0 = XXHash64::hash(&ldu, sizeof(ldu), seed_);
  const uint64_t h1 = XXHash64::hash(&ldu, sizeof(ldu), h0);
  return internal_query(h0, h1);
}

template<typename A>
bool bloom_filter_alloc<A>::query(float item) const {
  return query(static_cast<double>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query(const void* item, size_t size) const {
  if (item == nullptr || size == 0) return false;
  const uint64_t h0 = XXHash64::hash(item, size, seed_);
  const uint64_t h1 = XXHash64::hash(item, size, h0);
  return internal_query(h0, h1);
}

template<typename A>
bool bloom_filter_alloc<A>::internal_query(uint64_t h0, uint64_t h1) const {
  if (is_empty()) return false;
  const uint64_t num_bits = get_capacity();
  for (uint16_t i = 1; i <= num_hashes_; i++) {
    const uint64_t hash_index = ((h0 + i * h1) >> 1) % num_bits;
    if (!bit_array_ops::get_bit(bit_array_, hash_index))
      return false;
  }
  return true;
}

// OTHER METHODS

template<typename A>
bool bloom_filter_alloc<A>::is_compatible(const bloom_filter_alloc& other) const {
  return seed_ == other.seed_
    && num_hashes_ == other.num_hashes_
    && get_capacity() == other.get_capacity()
    ;
}

template<typename A>
void bloom_filter_alloc<A>::union_with(const bloom_filter_alloc& other) {
  if (!is_compatible(other)) {
    throw std::invalid_argument("Incompatible bloom filters");
  }
  uint64_t bits_set = bit_array_ops::union_with(bit_array_, other.bit_array_, capacity_bits_ >> 3);
  update_num_bits_set(bits_set);
}

template<typename A>
void bloom_filter_alloc<A>::intersect(const bloom_filter_alloc& other) {
  if (!is_compatible(other)) {
    throw std::invalid_argument("Incompatible bloom filters");
  }
  uint64_t bits_set = bit_array_ops::intersect(bit_array_, other.bit_array_, capacity_bits_ >> 3);
  update_num_bits_set(bits_set);
}

template<typename A>
void bloom_filter_alloc<A>::invert() {
  uint64_t bits_set = bit_array_ops::invert(bit_array_, capacity_bits_ >> 3);
  update_num_bits_set(bits_set);
}

template<typename A>
string<A> bloom_filter_alloc<A>::to_string(bool print_filter) const {
  // Using a temporary stream for implementation here does not comply with AllocatorAwareContainer requirements.
  // The stream does not support passing an allocator instance, and alternatives are complicated.
  std::ostringstream oss;
  uint64_t num_bits_set = num_bits_set_;
  if (is_dirty_) {
    num_bits_set = bit_array_ops::count_num_bits_set(bit_array_, capacity_bits_ >> 3);
  }

  oss << "### Bloom Filter Summary:" << std::endl;
  oss << "   num_bits   : " << get_capacity() << std::endl;
  oss << "   num_hashes : " << num_hashes_ << std::endl;
  oss << "   seed       : " << seed_ << std::endl;
  oss << "   is_dirty   : " << (is_dirty_ ? "true" : "false") << std::endl;
  oss << "   bits_used  : " << num_bits_set << std::endl;
  oss << "   fill %     : " << (num_bits_set * 100.0) / get_capacity() << std::endl;
  oss << "### End filter summary" << std::endl;

  if (print_filter) {
    uint64_t num_blocks = capacity_bits_ >> 6; // groups of 64 bits
    for (uint64_t i = 0; i < num_blocks; ++i) {
      oss << i << ": ";
      for (uint64_t j = 0; j < 8; ++j) { // bytes w/in a block
        for (uint64_t b = 0; b < 8; ++b) { // bits w/in a byte
          oss << ((bit_array_[i * 8 + j] & (1 << b)) ? "1" : "0");
        }
        oss << " ";
      }
      oss << std::endl;
    }
    oss << std::endl;
  }

  oss << std::endl;
  return string<A>(oss.str(), allocator_);
}


} // namespace datasketches

#endif // _BLOOM_FILTER_IMPL_HPP_