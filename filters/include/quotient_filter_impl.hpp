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

#ifndef QUOTIENT_FILTER_IMPL_HPP_
#define QUOTIENT_FILTER_IMPL_HPP_

#include <sstream>
#include <iomanip>
#include <queue>

#include "MurmurHash3.h"

namespace datasketches {

template<typename T>
T u64_to_hold_bits(T bits) {
  static_assert(std::is_integral<T>::value, "integral type expected");
  return (bits >> 6) + ((bits & 0x3f) > 0);
}

static inline void put_bits(uint64_t value, uint8_t bits, uint8_t* ptr, uint8_t offset) {
  if (offset > 0) {
    const uint8_t chunk = 8 - offset;
    if (bits < chunk) {
      const uint8_t mask = ((1 << bits) - 1) << offset;
      *ptr &= ~mask;
      *ptr |= (value << offset) & mask;
      return;
    }
    const uint8_t mask = ((1 << chunk) - 1) << offset;
    *ptr &= ~mask;
    *ptr++ |= (value << offset) & mask;
    bits -= chunk;
    value >>= chunk;
  }
  while (bits >= 8) {
    *ptr++ = value;
    bits -= 8;
    value >>= 8;
  }
  if (bits > 0) {
    const uint8_t mask = (1 << bits) - 1;
    *ptr &= ~mask;
    *ptr |= value & mask;
  }
}

static inline uint64_t get_bits(uint8_t bits, const uint8_t* ptr, uint8_t offset) {
  const uint8_t avail_bits = 8 - offset;
  const uint8_t chunk_bits = std::min(avail_bits, bits);
  const uint8_t mask = ((1 << chunk_bits) - 1);
  uint64_t value = 0;
  value = (*ptr >> offset) & mask;
  ptr += avail_bits == chunk_bits;
  offset = chunk_bits;
  bits -= chunk_bits;
  while (bits >= 8) {
    value |= static_cast<uint64_t>(*ptr++) << offset;
    bits -= 8;
    offset += 8;
  }
  if (bits > 0) {
    const uint8_t mask = (1 << bits) - 1;
    value |= static_cast<uint64_t>(*ptr & mask) << offset;
  }
  return value;
}

template<typename A>
quotient_filter_alloc<A>::quotient_filter_alloc(uint8_t lg_q, uint8_t num_fingerprint_bits, float load_factor, const A& allocator):
allocator_(allocator),
lg_q_(lg_q),
num_fingerprint_bits_(num_fingerprint_bits),
num_expansions_(0),
load_factor_(load_factor),
num_entries_(0),
bytes_(allocator)
{
  // check input
  // allocate multiples of 8 bytes to match Java
  bytes_.resize(u64_to_hold_bits(get_q() * get_num_bits_per_entry()) * sizeof(uint64_t));
}

template<typename A>
bool quotient_filter_alloc<A>::update(uint64_t value) {
  return update(&value, sizeof(value));
}

template<typename A>
bool quotient_filter_alloc<A>::update(const void* data, size_t length) {
  HashState hashes;
  MurmurHash3_x64_128(data, length, DEFAULT_SEED, hashes);
  const size_t quotient = quotient_from_hash(hashes.h1);
  const uint64_t remainder = value_from_hash(hashes.h1);
  return insert(quotient, remainder);
}

template<typename A>
bool quotient_filter_alloc<A>::insert(size_t quotient, uint64_t value) {
  const size_t run_start = find_run_start(quotient);
  if (!get_is_occupied(quotient)) {
    insert_and_shift(quotient, run_start, value, true, true);
    return true;
  }
  const auto pair = find_in_run(run_start, value);
  if (pair.second) return false;
  insert_and_shift(quotient, pair.first, value, false, pair.first == run_start);
  return true;
}

template<typename A>
bool quotient_filter_alloc<A>::query(uint64_t value) const {
  return query(&value, sizeof(value));
}

template<typename A>
bool quotient_filter_alloc<A>::query(const void* data, size_t length) const {
  HashState hashes;
  MurmurHash3_x64_128(data, length, DEFAULT_SEED, hashes);
  const size_t quotient = quotient_from_hash(hashes.h1);
  if (!get_is_occupied(quotient)) return false;
  const size_t run_start = find_run_start(quotient);
  const uint64_t remainder = value_from_hash(hashes.h1);
  const auto pair = find_in_run(run_start, remainder);
  return pair.second;
}

template<typename A>
void quotient_filter_alloc<A>::merge(const quotient_filter_alloc& other) {
  if (lg_q_ + num_fingerprint_bits_ != other.lg_q_ + other.num_fingerprint_bits_) {
    throw std::invalid_argument("incompatible sketches in merge");
  }
  // find cluster start
  size_t i = 0;
  if (!other.is_slot_empty(i)) while (other.get_is_shifted(i)) i = (i - 1) & other.get_slot_mask();

  std::queue<size_t> fifo;
  size_t count = 0;
  while (count < other.num_entries_) {
    if (!other.is_slot_empty(i)) {
      if (other.get_is_occupied(i)) fifo.push(i);
      const size_t quotient = fifo.front();
      const uint64_t value = other.get_value(i);
      const uint64_t hash = quotient << other.num_fingerprint_bits_ | value;
      insert(quotient_from_hash(hash), value_from_hash(hash));
      ++count;
    }
    i = (i + 1) & other.get_slot_mask();
    if (!fifo.empty() && !other.get_is_continuation(i)) fifo.pop();
  }
}

template<typename A>
size_t quotient_filter_alloc<A>::get_q() const {
  return static_cast<size_t>(1) << get_lg_q();
}

template<typename A>
size_t quotient_filter_alloc<A>::get_slot_mask() const {
  return get_q() - 1;
}

template<typename A>
uint64_t quotient_filter_alloc<A>::get_value_mask() const {
  return (static_cast<uint64_t>(1) << get_num_bits_in_value()) - 1;
}

template<typename A>
size_t quotient_filter_alloc<A>::quotient_from_hash(uint64_t hash) const {
  return (hash >> get_num_bits_in_value()) & get_slot_mask();
}

template<typename A>
uint64_t quotient_filter_alloc<A>::value_from_hash(uint64_t hash) const {
  return hash & get_value_mask();
}

template<typename A>
size_t quotient_filter_alloc<A>::get_num_entries() const {
  return num_entries_;
}

template<typename A>
uint8_t quotient_filter_alloc<A>::get_lg_q() const {
  return lg_q_;
}

template<typename A>
uint8_t quotient_filter_alloc<A>::get_num_bits_per_entry() const {
  return num_fingerprint_bits_ + 3;
}

template<typename A>
uint8_t quotient_filter_alloc<A>::get_num_bits_in_value() const {
  return num_fingerprint_bits_;
}

template<typename A>
uint8_t quotient_filter_alloc<A>::get_num_expansions() const {
  return num_expansions_;
}

template<typename A>
A quotient_filter_alloc<A>::get_allocator() const {
  return allocator_;
}

template<typename A>
string<A> quotient_filter_alloc<A>::to_string(bool print_entries) const {
  // Using a temporary stream for implementation here does not comply with AllocatorAwareContainer requirements.
  // The stream does not support passing an allocator instance, and alternatives are complicated.
  std::ostringstream os;
  os << "### Quotient filter summary:" << std::endl;
  os << "   LgQ              : " << std::to_string(lg_q_) << std::endl;
  os << "   Fingerprint bits : " << std::to_string(num_fingerprint_bits_) << std::endl;
  os << "   Load factor      : " << std::to_string(load_factor_) << std::endl;
  os << "   Num expansions   : " << std::to_string(num_expansions_) << std::endl;
  os << "   Num entries      : " << num_entries_ << std::endl;
  os << "### End filter summary" << std::endl;

  if (print_entries) {
    os << "### Quotient filter entries:" << std::endl;
    for (size_t slot = 0; slot < get_q(); ++slot) {
      os << slot << ": ";
      os << (get_is_occupied(slot) ? "1" : "0");
      os << (get_is_continuation(slot) ? "1" : "0");
      os << (get_is_shifted(slot) ? "1" : "0");
      os << " ";
      os << std::hex << get_value(slot) << std::dec;
      os << std::endl;
    }

//    for (size_t bit = 0; bit < get_q() * bits_per_entry_; ++bit) {
//      size_t remainder = bit % bits_per_entry_;
//      if (remainder == 0) {
//        size_t slot = bit / bits_per_entry_;
//        os << slot << ": ";
//      }
//      if (remainder == 3) os << " ";
//      os << (get_bit(bit) ? "1" : "0");
//      if (remainder == bits_per_entry_ - 1) os << "\n";
//    }

    os << "### End filter entries" << std::endl;
  }
  return string<A>(os.str().c_str(), get_allocator());
}

template<typename A>
size_t quotient_filter_alloc<A>::find_run_start(size_t slot) const {
  size_t num_runs_to_skip = 0;
  while (get_is_shifted(slot)) {
    slot = (slot - 1) & get_slot_mask();
    if (get_is_occupied(slot)) ++num_runs_to_skip;
  }
  while (num_runs_to_skip > 0) {
    slot = (slot + 1) & get_slot_mask();
    if (!get_is_continuation(slot)) --num_runs_to_skip;
  }
  return slot;
}

template<typename A>
std::pair<size_t, bool> quotient_filter_alloc<A>::find_in_run(size_t slot, uint64_t value) const {
  do {
    const uint64_t value_from_entry = get_value(slot);
    if (value_from_entry >= value) return std::make_pair(slot, value_from_entry == value);
    slot = (slot + 1) & get_slot_mask();
  } while (get_is_continuation(slot));
  return std::make_pair(slot, false);
}

template<typename A>
void quotient_filter_alloc<A>::insert_and_shift(size_t quotient, size_t slot, uint64_t value, bool is_new_run, bool is_run_start) {
//  std::cout << "insert " << quotient << ":" << std::hex << value << std::dec << " at " << slot << " as " << (is_new_run ? "new run\n" : "existing run\n");

  // in the first shifted entry set is_continuation flag if inserting at the start of the existing run
  // otherwise just shift the existing flag as it is
  bool force_continuation = !is_new_run && is_run_start;

  // prepare flags for the current slot
  bool is_continuation = !is_run_start;
  bool is_shifted = slot != quotient;

  // remember the existing entry from the current slot to be shifted to the next slot
  // is_occupied flag belongs to the slot, therefore it is never shifted
  // is_shifted flag is always true for all shifted entries, no need to remember it
  uint64_t existing_value = get_value(slot);
  bool existing_is_continuation = get_is_continuation(slot);

  while (!is_slot_empty(slot)) {
    // set the current slot
    set_value(slot, value);
    set_is_continuation(slot, is_continuation);
    set_is_shifted(slot, is_shifted);

    // prepare values for the next slot
    value = existing_value;
    is_continuation = existing_is_continuation | force_continuation;
    is_shifted = true;

    slot = (slot + 1) & get_slot_mask();

    // remember the existing entry to be shifted
    existing_value = get_value(slot);
    existing_is_continuation = get_is_continuation(slot);

    force_continuation = false; // this is needed for the first shift only
  }
  // at this point the current slot is empty, so just populate with prepared values
  // either the incoming value or the last shifted one
  set_value(slot, value);
  set_is_continuation(slot, is_continuation);
  set_is_shifted(slot, is_shifted);

  if (is_new_run) set_is_occupied(quotient, true);
  ++num_entries_;
  if (num_entries_ == static_cast<size_t>(get_q() * load_factor_)) expand();
}

template<typename A>
void quotient_filter_alloc<A>::expand() {
  if (get_num_bits_in_value() < 2) throw std::logic_error("for expansion value must have at least 2 bits");
  quotient_filter_alloc other(lg_q_ + 1, num_fingerprint_bits_ - 1, load_factor_, allocator_);

  // find cluster start
  size_t i = 0;
  if (!is_slot_empty(i)) while (get_is_shifted(i)) i = (i - 1) & get_slot_mask();

  std::queue<size_t> fifo;
  size_t count = 0;
  while (count < num_entries_) {
    if (!is_slot_empty(i)) {
      if (get_is_occupied(i)) fifo.push(i);
      const uint64_t value = get_value(i);
      const size_t new_quotient = (fifo.front() << 1) | (value >> other.get_num_bits_in_value());
      other.insert(new_quotient, value & other.get_value_mask());
      ++count;
    }
    i = (i + 1) & get_slot_mask();
    if (!fifo.empty() && !get_is_continuation(i)) fifo.pop();
  }
  std::swap(*this, other);
  num_expansions_ = other.num_expansions_ + 1;
}

template<typename A>
bool quotient_filter_alloc<A>::get_bit(size_t bit_index) const {
  const size_t byte_offset = bit_index >> 3;
  const uint8_t bit_offset = bit_index & 7;
  const uint8_t* ptr = bytes_.data() + byte_offset;
  return *ptr & (1 << bit_offset);
}

template<typename A>
bool quotient_filter_alloc<A>::get_is_occupied(size_t slot) const {
  return get_bit(slot * get_num_bits_per_entry());
}

template<typename A>
bool quotient_filter_alloc<A>::get_is_continuation(size_t slot) const {
  return get_bit(slot * get_num_bits_per_entry() + 1);
}

template<typename A>
bool quotient_filter_alloc<A>::get_is_shifted(size_t slot) const {
  return get_bit(slot * get_num_bits_per_entry() + 2);
}

template<typename A>
bool quotient_filter_alloc<A>::is_slot_empty(size_t slot) const {
  return !get_is_occupied(slot) && !get_is_continuation(slot) && !get_is_shifted(slot);
}

template<typename A>
uint64_t quotient_filter_alloc<A>::get_value(size_t slot) const {
  const size_t bits = slot * get_num_bits_per_entry() + 3;
  const size_t byte_offset = bits >> 3;
  const uint8_t bit_offset = bits & 7;
  const uint8_t* ptr = bytes_.data() + byte_offset;
  return get_bits(get_num_bits_in_value(), ptr, bit_offset);
}

template<typename A>
void quotient_filter_alloc<A>::set_bit(size_t bit_index, bool state) {
  const size_t byte_offset = bit_index >> 3;
  const uint8_t bit_offset = bit_index & 7;
  uint8_t* ptr = bytes_.data() + byte_offset;
  if (state) {
    *ptr |= 1 << bit_offset;
  } else {
    *ptr &= ~(1 << bit_offset);
  }
}

template<typename A>
void quotient_filter_alloc<A>::set_is_occupied(size_t slot, bool state) {
  set_bit(slot * get_num_bits_per_entry(), state);
}

template<typename A>
void quotient_filter_alloc<A>::set_is_continuation(size_t slot, bool state) {
  set_bit(slot * get_num_bits_per_entry() + 1, state);
}

template<typename A>
void quotient_filter_alloc<A>::set_is_shifted(size_t slot, bool state) {
  set_bit(slot * get_num_bits_per_entry() + 2, state);
}

template<typename A>
void quotient_filter_alloc<A>::set_value(size_t slot, uint64_t value) {
  const size_t bits = slot * get_num_bits_per_entry() + 3;
  const size_t byte_offset = bits >> 3;
  const uint8_t bit_offset = bits & 7;
  uint8_t* ptr = bytes_.data() + byte_offset;
  put_bits(value, get_num_bits_in_value(), ptr, bit_offset);
}

template<typename A>
void quotient_filter_alloc<A>::serialize(std::ostream& os) const {
  write<uint64_t>(os, 0); // placeholders, to be implemented
  write<uint64_t>(os, 0);
  write<uint64_t>(os, 0);
  write<uint64_t>(os, 0);
  write(os, bytes_.data(), bytes_.size());
}

} /* namespace datasketches */

#endif
