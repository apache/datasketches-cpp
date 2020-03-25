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

#ifndef _VAR_OPT_SKETCH_IMPL_HPP_
#define _VAR_OPT_SKETCH_IMPL_HPP_

#include "var_opt_sketch.hpp"
#include "serde.hpp"
#include "bounds_binomial_proportions.hpp"

#include <memory>
#include <sstream>
#include <cmath>
#include <random>
#include <algorithm>

namespace datasketches {

/**
 * Implementation code for the VarOpt sketch.
 * 
 * author Kevin Lang 
 * author Jon Malkin
 */
template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>::var_opt_sketch(uint32_t k, resize_factor rf) :
  var_opt_sketch<T,S,A>(k, rf, false) {}

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>::var_opt_sketch(const var_opt_sketch& other) :
  k_(other.k_),
  h_(other.h_),
  m_(other.m_),
  r_(other.r_),
  n_(other.n_),
  total_wt_r_(other.total_wt_r_),
  rf_(other.rf_),
  curr_items_alloc_(other.curr_items_alloc_),
  filled_data_(other.filled_data_),
  data_(nullptr),
  weights_(nullptr),
  num_marks_in_h_(other.num_marks_in_h_),
  marks_(nullptr)
  {
    data_ = A().allocate(curr_items_alloc_);
    if (other.filled_data_) {
      // copy everything
      for (size_t i = 0; i < curr_items_alloc_; ++i)
        A().construct(&data_[i], T(other.data_[i]));
    } else {
      // skip gap or anything unused at the end
      for (size_t i = 0; i < h_; ++i)
        A().construct(&data_[i], T(other.data_[i]));
      for (size_t i = h_ + 1; i < h_ + r_ + 1; ++i)
        A().construct(&data_[i], T(other.data_[i]));
    }
    
    weights_ = AllocDouble().allocate(curr_items_alloc_);
    // doubles so can successfully copy regardless of the internal state
    std::copy(&other.weights_[0], &other.weights_[curr_items_alloc_], weights_);
    
    if (other.marks_ != nullptr) {
      marks_ = AllocBool().allocate(curr_items_alloc_);
      std::copy(&other.marks_[0], &other.marks_[curr_items_alloc_], marks_);
    }
  }

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>::var_opt_sketch(const var_opt_sketch& other, bool as_sketch, uint64_t adjusted_n) :
  k_(other.k_),
  h_(other.h_),
  m_(other.m_),
  r_(other.r_),
  n_(adjusted_n),
  total_wt_r_(other.total_wt_r_),
  rf_(other.rf_),
  curr_items_alloc_(other.curr_items_alloc_),
  filled_data_(other.filled_data_),
  data_(nullptr),
  weights_(nullptr),
  num_marks_in_h_(other.num_marks_in_h_),
  marks_(nullptr)
  {
    data_ = A().allocate(curr_items_alloc_);
    if (other.filled_data_) {
      // copy everything
      for (size_t i = 0; i < curr_items_alloc_; ++i)
        A().construct(&data_[i], T(other.data_[i]));
    } else {
      // skip gap or anything unused at the end
      for (size_t i = 0; i < h_; ++i)
        A().construct(&data_[i], T(other.data_[i]));
      for (size_t i = h_ + 1; i < h_ + r_ + 1; ++i)
        A().construct(&data_[i], T(other.data_[i]));
    }

    weights_ = AllocDouble().allocate(curr_items_alloc_);
    // doubles so can successfully copy regardless of the internal state
    std::copy(&other.weights_[0], &other.weights_[curr_items_alloc_], weights_);

    if (!as_sketch && other.marks_ != nullptr) {
      marks_ = AllocBool().allocate(curr_items_alloc_);
      std::copy(&other.marks_[0], &other.marks_[curr_items_alloc_], marks_);
    }
  }

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>::var_opt_sketch(T* data, double* weights, size_t len,
                                      uint32_t k, uint64_t n, uint32_t h_count, uint32_t r_count, double total_wt_r) :
  k_(k),
  h_(h_count),
  m_(0),
  r_(r_count),
  n_(n),
  total_wt_r_(total_wt_r),
  rf_(DEFAULT_RESIZE_FACTOR),
  curr_items_alloc_(len),
  filled_data_(n > k),
  data_(data),
  weights_(weights),
  num_marks_in_h_(0),
  marks_(nullptr)
  {}

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>::var_opt_sketch(var_opt_sketch&& other) noexcept :
  k_(other.k_),
  h_(other.h_),
  m_(other.m_),
  r_(other.r_),
  n_(other.n_),
  total_wt_r_(other.total_wt_r_),
  rf_(other.rf_),
  curr_items_alloc_(other.curr_items_alloc_),
  filled_data_(other.filled_data_),
  data_(other.data_),
  weights_(other.weights_),
  num_marks_in_h_(other.num_marks_in_h_),
  marks_(other.marks_)
  {
    other.data_ = nullptr;
    other.weights_ = nullptr;
    other.marks_ = nullptr;
  }

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>::var_opt_sketch(uint32_t k, resize_factor rf, bool is_gadget) :
  k_(k), h_(0), m_(0), r_(0), n_(0), total_wt_r_(0.0), rf_(rf) {
  if (k == 0 || k_ > MAX_K) {
    throw std::invalid_argument("k must be at least 1 and less than 2^31 - 1");
  }

  uint32_t ceiling_lg_k = to_log_2(ceiling_power_of_2(k_));
  int initial_lg_size = starting_sub_multiple(ceiling_lg_k, rf_, MIN_LG_ARR_ITEMS);
  curr_items_alloc_ = get_adjusted_size(k_, 1 << initial_lg_size);
  if (curr_items_alloc_ == k_) { // if full size, need to leave 1 for the gap
    ++curr_items_alloc_;
  }

  allocate_data_arrays(curr_items_alloc_, is_gadget);
  num_marks_in_h_ = 0;
}

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>::~var_opt_sketch() {
  if (data_ != nullptr) {
    if (filled_data_) {
      // destroy everything
      for (size_t i = 0; i < curr_items_alloc_; ++i) {
        A().destroy(data_ + i);      
      }
    } else {
      // skip gap or anything unused at the end
      for (size_t i = 0; i < h_; ++i) {
        A().destroy(data_+ i);
      }
    
      for (size_t i = h_ + 1; i < h_ + r_ + 1; ++i) {
        A().destroy(data_ + i);
      }
    }
    A().deallocate(data_, curr_items_alloc_);
  }

  if (weights_ != nullptr) {
    AllocDouble().deallocate(weights_, curr_items_alloc_);
  }
  
  if (marks_ != nullptr) {
    AllocBool().deallocate(marks_, curr_items_alloc_);
  }
}

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>& var_opt_sketch<T,S,A>::operator=(const var_opt_sketch& other) {
  var_opt_sketch<T,S,A> sk_copy(other);
  std::swap(k_, sk_copy.k_);
  std::swap(h_, sk_copy.h_);
  std::swap(m_, sk_copy.m_);
  std::swap(r_, sk_copy.r_);
  std::swap(n_, sk_copy.n_);
  std::swap(total_wt_r_, sk_copy.total_wt_r_);
  std::swap(rf_, sk_copy.rf_);
  std::swap(curr_items_alloc_, sk_copy.curr_items_alloc_);
  std::swap(filled_data_, sk_copy.filled_data_);
  std::swap(data_, sk_copy.data_);
  std::swap(weights_, sk_copy.weights_);
  std::swap(num_marks_in_h_, sk_copy.num_marks_in_h_);
  std::swap(marks_, sk_copy.marks_);
  return *this;
}

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>& var_opt_sketch<T,S,A>::operator=(var_opt_sketch&& other) {
  std::swap(k_, other.k_);
  std::swap(h_, other.h_);
  std::swap(m_, other.m_);
  std::swap(r_, other.r_);
  std::swap(n_, other.n_);
  std::swap(total_wt_r_, other.total_wt_r_);
  std::swap(rf_, other.rf_);
  std::swap(curr_items_alloc_, other.curr_items_alloc_);
  std::swap(filled_data_, other.filled_data_);
  std::swap(data_, other.data_);
  std::swap(weights_, other.weights_);
  std::swap(num_marks_in_h_, other.num_marks_in_h_);
  std::swap(marks_, other.marks_);
  return *this;
}

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>::var_opt_sketch(uint32_t k, resize_factor rf, bool is_gadget, uint8_t preamble_longs, std::istream& is) :
  k_(k), m_(0), rf_(rf) {

  // second and third prelongs
  is.read((char*)&n_, sizeof(uint64_t));
  is.read((char*)&h_, sizeof(uint32_t));
  is.read((char*)&r_, sizeof(uint32_t));

  validate_and_set_current_size(preamble_longs);

  // current_items_alloc_ is set but validate R region weight (4th prelong), if needed, before allocating
  if (preamble_longs == PREAMBLE_LONGS_FULL) { 
    is.read((char*)&total_wt_r_, sizeof(total_wt_r_));
    if (isnan(total_wt_r_) || r_ == 0 || total_wt_r_ <= 0.0) {
      throw std::invalid_argument("Possible corruption: deserializing in full mode but r = 0 or invalid R weight. "
       "Found r = " + std::to_string(r_) + ", R region weight = " + std::to_string(total_wt_r_));
    }
  } else {
    total_wt_r_ = 0.0;
  }

  allocate_data_arrays(curr_items_alloc_, is_gadget);

  // read the first h_ weights
  is.read((char*)weights_, h_ * sizeof(double));
  for (size_t i = 0; i < h_; ++i) {
    if (!(weights_[i] > 0.0)) {
      throw std::invalid_argument("Possible corruption: Non-positive weight when deserializing: " + std::to_string(weights_[i]));
    }
  }

  std::fill(&weights_[h_], &weights_[curr_items_alloc_], -1.0);

  // read the first h_ marks as packed bytes iff we have a gadget
  num_marks_in_h_ = 0;
  if (is_gadget) {
    uint8_t val = 0;
    for (int i = 0; i < h_; ++i) {
      if ((i & 0x7) == 0x0) { // should trigger on first iteration
        is.read((char*)&val, sizeof(val));
      }
      marks_[i] = ((val >> (i & 0x7)) & 0x1) == 1;
      num_marks_in_h_ += (marks_[i] ? 1 : 0);
    }
  }

  // read the sample items, skipping the gap. Either h_ or r_ may be 0
  S().deserialize(is, data_, h_); // aka &data_[0]
  S().deserialize(is, &data_[h_ + 1], r_);
}

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>::var_opt_sketch(uint32_t k, resize_factor rf, bool is_gadget, uint8_t preamble_longs,
                                      const void* bytes, size_t size) : k_(k), m_(0), rf_(rf) {
  // private constructor so we assume not called if sketch is empty
  const char* ptr = static_cast<const char*>(bytes) + sizeof(uint64_t);

  // second and third prelongs
  ptr += copy_from_mem(ptr, &n_, sizeof(n_));
  ptr += copy_from_mem(ptr, &h_, sizeof(h_));
  ptr += copy_from_mem(ptr, &r_, sizeof(r_));

  validate_and_set_current_size(preamble_longs);
  
  // current_items_alloc_ is set but validate R region weight (4th prelong), if needed, before allocating
  if (preamble_longs == PREAMBLE_LONGS_FULL) {
    ptr += copy_from_mem(ptr, &total_wt_r_, sizeof(total_wt_r_));
    if (isnan(total_wt_r_) || r_ == 0 || total_wt_r_ <= 0.0) {
      throw std::invalid_argument("Possible corruption: deserializing in full mode but r = 0 or invalid R weight. "
       "Found r = " + std::to_string(r_) + ", R region weight = " + std::to_string(total_wt_r_));
    }
  } else {
    total_wt_r_ = 0.0;
  }

  allocate_data_arrays(curr_items_alloc_, is_gadget);

  // read the first h_ weights, fill in rest of array with -1.0
  ptr += copy_from_mem(ptr, weights_, h_ * sizeof(double));
  for (size_t i = 0; i < h_; ++i) {
    if (!(weights_[i] > 0.0)) {
      throw std::invalid_argument("Possible corruption: Non-positive weight when deserializing: " + std::to_string(weights_[i]));
    }
  }
  std::fill(&weights_[h_], &weights_[curr_items_alloc_], -1.0);
  
  // read the first h_ marks as packed bytes iff we have a gadget
  num_marks_in_h_ = 0;
  if (is_gadget) {
    uint8_t val = 0;
    for (int i = 0; i < h_; ++i) {
     if ((i & 0x7) == 0x0) { // should trigger on first iteration
        ptr += copy_from_mem(ptr, &val, sizeof(val));
      }
      marks_[i] = ((val >> (i & 0x7)) & 0x1) == 1;
      num_marks_in_h_ += (marks_[i] ? 1 : 0);
    }
  }

  // read the sample items, skipping the gap. Either h_ or r_ may be 0
  ptr += S().deserialize(ptr, data_, h_); // ala data_[0]
  ptr += S().deserialize(ptr, &data_[h_ + 1], r_);
}

/*
 * An empty sketch requires 8 bytes.
 *
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||       0        |    1   |    2   |    3   |    4   |    5   |    6   |    7   |
 *  0   || Preamble_Longs | SerVer | FamID  |  Flags |---------Max Res. Size (K)---------|
 * </pre>
 *
 * A non-empty sketch requires 24 bytes of preamble for an under-full sample; once there are
 * at least k items the sketch uses 32 bytes of preamble.
 *
 * The count of items seen is limited to 48 bits (~256 trillion) even though there are adjacent
 * unused preamble bits. The acceptance probability for an item is a double in the range [0,1),
 * limiting us to 53 bits of randomness due to details of the IEEE floating point format. To
 * ensure meaningful probabilities as the items seen count approaches capacity, we intentionally
 * use slightly fewer bits.
 * 
 * Following the header are weights for the heavy items, then marks in the event this is a gadget.
 * The serialized items come last.
 * 
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||       0        |    1   |    2   |    3   |    4   |    5   |    6   |    7   |
 *  0   || Preamble_Longs | SerVer | FamID  |  Flags |---------Max Res. Size (K)---------|
 *
 *      ||       8        |    9   |   10   |   11   |   12   |   13   |   14   |   15   |
 *  1   ||---------------------------Items Seen Count (N)--------------------------------|
 *
 *      ||      16        |   17   |   18   |   19   |   20   |   21   |   22   |   23   |
 *  2   ||-------------Item Count in H---------------|-------Item Count in R-------------|
 *
 *      ||      24        |   25   |   26   |   27   |   28   |   29   |   30   |   31   |
 *  3   ||-------------------------------Total Weight in R-------------------------------|
 * </pre>
 */

// implementation for fixed-size arithmetic types (integral and floating point)
template<typename T, typename S, typename A>
template<typename TT, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type>
size_t var_opt_sketch<T,S,A>::get_serialized_size_bytes() const {
  if (is_empty()) { return PREAMBLE_LONGS_EMPTY << 3; }
  size_t num_bytes = (r_ == 0 ? PREAMBLE_LONGS_WARMUP : PREAMBLE_LONGS_FULL) << 3;
  num_bytes += h_ * sizeof(double);    // weights
  if (marks_ != nullptr) {             // marks
    num_bytes += (h_ / 8) + (h_ % 8 > 0);
  }
  num_bytes += (h_ + r_) * sizeof(TT); // the actual items
  return num_bytes;
}

// implementation for all other types
template<typename T, typename S, typename A>
template<typename TT, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type>
size_t var_opt_sketch<T,S,A>::get_serialized_size_bytes() const {
  if (is_empty()) { return PREAMBLE_LONGS_EMPTY << 3; }
  size_t num_bytes = (r_ == 0 ? PREAMBLE_LONGS_WARMUP : PREAMBLE_LONGS_FULL) << 3;
  num_bytes += h_ * sizeof(double);    // weights
  if (marks_ != nullptr) {             // marks
    num_bytes += (h_ / 8) + (h_ % 8 > 0);
  }
  // must iterate over the items
  for (auto& it: *this)
    num_bytes += S().size_of_item(it.first);
  return num_bytes;
}

template<typename T, typename S, typename A>
std::vector<uint8_t, AllocU8<A>> var_opt_sketch<T,S,A>::serialize(unsigned header_size_bytes) const {
  const size_t size = header_size_bytes + get_serialized_size_bytes();
  std::vector<uint8_t, AllocU8<A>> bytes(size);
  uint8_t* ptr = bytes.data() + header_size_bytes;

  bool empty = is_empty();
  uint8_t preLongs = (empty ? PREAMBLE_LONGS_EMPTY
                                  : (r_ == 0 ? PREAMBLE_LONGS_WARMUP : PREAMBLE_LONGS_FULL));
  uint8_t first_byte = (preLongs & 0x3F) | ((static_cast<uint8_t>(rf_)) << 6);
  uint8_t flags = (marks_ != nullptr ? GADGET_FLAG_MASK : 0);

  if (empty) {
    flags |= EMPTY_FLAG_MASK;
  }

  // first prelong
  uint8_t ser_ver(SER_VER);
  uint8_t family(FAMILY_ID);
  ptr += copy_to_mem(&first_byte, ptr, sizeof(uint8_t));
  ptr += copy_to_mem(&ser_ver, ptr, sizeof(uint8_t));
  ptr += copy_to_mem(&family, ptr, sizeof(uint8_t));
  ptr += copy_to_mem(&flags, ptr, sizeof(uint8_t));
  ptr += copy_to_mem(&k_, ptr, sizeof(uint32_t));

  if (!empty) {
    // second and third prelongs
    ptr += copy_to_mem(&n_, ptr, sizeof(uint64_t));
    ptr += copy_to_mem(&h_, ptr, sizeof(uint32_t));
    ptr += copy_to_mem(&r_, ptr, sizeof(uint32_t));

    // fourth prelong, if needed
    if (r_ > 0) {
      ptr += copy_to_mem(&total_wt_r_, ptr, sizeof(double));
    }

    // first h_ weights
    ptr += copy_to_mem(weights_, ptr, h_ * sizeof(double));

    // first h_ marks as packed bytes iff we have a gadget
    if (marks_ != nullptr) {
      uint8_t val = 0;
      for (int i = 0; i < h_; ++i) {
        if (marks_[i]) {
          val |= 0x1 << (i & 0x7);
        }

        if ((i & 0x7) == 0x7) {
          ptr += copy_to_mem(&val, ptr, sizeof(uint8_t));
          val = 0;
        }
      }

      // write out any remaining values
      if ((h_ & 0x7) > 0) {
        ptr += copy_to_mem(&val, ptr, sizeof(uint8_t));
      }
    }

    // write the sample items, skipping the gap. Either h_ or r_ may be 0
    ptr += S().serialize(ptr, data_, h_);
    ptr += S().serialize(ptr, &data_[h_ + 1], r_);
  }
  
  size_t bytes_written = ptr - bytes.data();
  if (bytes_written != size) {
    throw std::logic_error("serialized size mismatch: " + std::to_string(bytes_written) + " != " + std::to_string(size));
  }

  return bytes;
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::serialize(std::ostream& os) const {
  const bool empty = (h_ == 0) && (r_ == 0);

  const uint8_t preLongs = (empty ? PREAMBLE_LONGS_EMPTY
                                  : (r_ == 0 ? PREAMBLE_LONGS_WARMUP : PREAMBLE_LONGS_FULL));
  const uint8_t first_byte = (preLongs & 0x3F) | ((static_cast<uint8_t>(rf_)) << 6);
  uint8_t flags = (marks_ != nullptr ? GADGET_FLAG_MASK : 0);

  if (empty) {
    flags |= EMPTY_FLAG_MASK;
  }

  // first prelong
  const uint8_t ser_ver(SER_VER);
  const uint8_t family(FAMILY_ID);
  os.write((char*)&first_byte, sizeof(uint8_t));
  os.write((char*)&ser_ver, sizeof(uint8_t));
  os.write((char*)&family, sizeof(uint8_t));
  os.write((char*)&flags, sizeof(uint8_t));
  os.write((char*)&k_, sizeof(uint32_t));

  if (!empty) {
    // second and third prelongs
    os.write((char*)&n_, sizeof(uint64_t));
    os.write((char*)&h_, sizeof(uint32_t));
    os.write((char*)&r_, sizeof(uint32_t));
    
    // fourth prelong, if needed
    if (r_ > 0) {
      os.write((char*)&total_wt_r_, sizeof(double));
    }

    // write the first h_ weights
    os.write((char*)weights_, h_ * sizeof(double));

    // write the first h_ marks as packed bytes iff we have a gadget
    if (marks_ != nullptr) {
      uint8_t val = 0;
      for (int i = 0; i < h_; ++i) {
        if (marks_[i]) {
          val |= 0x1 << (i & 0x7);
        }

        if ((i & 0x7) == 0x7) {
          os.write((char*)&val, sizeof(uint8_t));
          val = 0;
        }
      }

      // write out any remaining values
      if ((h_ & 0x7) > 0) {
        os.write((char*)&val, sizeof(uint8_t));
      }
    }

    // write the sample items, skipping the gap. Either h_ or r_ may be 0
    S().serialize(os, data_, h_);
    S().serialize(os, &data_[h_ + 1], r_);
  }
}

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A> var_opt_sketch<T,S,A>::deserialize(const void* bytes, size_t size) {
  const char* ptr = static_cast<const char*>(bytes);
  uint8_t first_byte;
  ptr += copy_from_mem(ptr, &first_byte, sizeof(first_byte));
  uint8_t preamble_longs = first_byte & 0x3f;
  resize_factor rf = static_cast<resize_factor>((first_byte >> 6) & 0x03);
  uint8_t serial_version;
  ptr += copy_from_mem(ptr, &serial_version, sizeof(serial_version));
  uint8_t family_id;
  ptr += copy_from_mem(ptr, &family_id, sizeof(family_id));
  uint8_t flags;
  ptr += copy_from_mem(ptr, &flags, sizeof(flags));
  uint32_t k;
  ptr += copy_from_mem(ptr, &k, sizeof(k));

  check_preamble_longs(preamble_longs, flags);
  check_family_and_serialization_version(family_id, serial_version);

  const bool is_empty = flags & EMPTY_FLAG_MASK;
  const bool is_gadget = flags & GADGET_FLAG_MASK;

  return is_empty ? var_opt_sketch<T,S,A>(k, rf, is_gadget) : var_opt_sketch<T,S,A>(k, rf, is_gadget, preamble_longs, bytes, size);
}

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A> var_opt_sketch<T,S,A>::deserialize(std::istream& is) {
  uint8_t first_byte;
  is.read((char*)&first_byte, sizeof(first_byte));
  uint8_t preamble_longs = first_byte & 0x3f;
  resize_factor rf = static_cast<resize_factor>((first_byte >> 6) & 0x03);
  uint8_t serial_version;
  is.read((char*)&serial_version, sizeof(serial_version));
  uint8_t family_id;
  is.read((char*)&family_id, sizeof(family_id));
  uint8_t flags;
  is.read((char*)&flags, sizeof(flags));
  uint32_t k;
  is.read((char*)&k, sizeof(k));

  check_preamble_longs(preamble_longs, flags);
  check_family_and_serialization_version(family_id, serial_version);

  const bool is_empty = flags & EMPTY_FLAG_MASK;
  const bool is_gadget = flags & GADGET_FLAG_MASK;

  return is_empty ? var_opt_sketch<T,S,A>(k, rf, is_gadget) : var_opt_sketch<T,S,A>(k, rf, is_gadget, preamble_longs, is);
}

template<typename T, typename S, typename A>
bool var_opt_sketch<T,S,A>::is_empty() const {
  return (h_ == 0 && r_ == 0);
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::reset() {
  const uint32_t prev_alloc = curr_items_alloc_;

  const uint32_t ceiling_lg_k = to_log_2(ceiling_power_of_2(k_));
  const int initial_lg_size = starting_sub_multiple(ceiling_lg_k, rf_, MIN_LG_ARR_ITEMS);
  curr_items_alloc_ = get_adjusted_size(k_, 1 << initial_lg_size);
  if (curr_items_alloc_ == k_) { // if full size, need to leave 1 for the gap
    ++curr_items_alloc_;
  }
  
  if (filled_data_) {
    // destroy everything
    for (size_t i = 0; i < curr_items_alloc_; ++i) 
      A().destroy(data_ + i);      
  } else {
    // skip gap or anything unused at the end
    for (size_t i = 0; i < h_; ++i)
      A().destroy(data_+ i);
    
    for (size_t i = h_ + 1; i < curr_items_alloc_; ++i)
      A().destroy(data_ + i);
  }

  if (curr_items_alloc_ < prev_alloc) {
    const bool is_gadget = (marks_ != nullptr);
  
    A().deallocate(data_, curr_items_alloc_);
    AllocDouble().deallocate(weights_, curr_items_alloc_);
  
    if (marks_ != nullptr)
      AllocBool().deallocate(marks_, curr_items_alloc_);

    allocate_data_arrays(curr_items_alloc_, is_gadget);
  }
  
  n_ = 0;
  h_ = 0;
  m_ = 0;
  r_ = 0;
  num_marks_in_h_ = 0;
  total_wt_r_ = 0.0;
  filled_data_ = false;
}

template<typename T, typename S, typename A>
uint64_t var_opt_sketch<T,S,A>::get_n() const {
  return n_;
}

template<typename T, typename S, typename A>
uint32_t var_opt_sketch<T,S,A>::get_k() const {
  return k_;
}

template<typename T, typename S, typename A>
uint32_t var_opt_sketch<T,S,A>::get_num_samples() const {
  const uint32_t num_in_sketch = h_ + r_;
  return (num_in_sketch < k_ ? num_in_sketch : k_);
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::update(const T& item, double weight) {
  update(item, weight, false);
}

/*
template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::update(T&& item, double weight) {
}
*/

template<typename T, typename S, typename A>
std::ostream& var_opt_sketch<T,S,A>::to_stream(std::ostream& os) const {
  os << "### VarOpt SUMMARY: " << std::endl;
  os << "   k            : " << k_ << std::endl;
  os << "   h            : " << h_ << std::endl;
  os << "   r            : " << r_ << std::endl;
  os << "   weight_r     : " << total_wt_r_ << std::endl;
  os << "   Current size : " << curr_items_alloc_ << std::endl;
  os << "   Resize factor: " << (1 << rf_) << std::endl;
  os << "### END SKETCH SUMMARY" << std::endl;

  return os;
}

template<typename T, typename S, typename A>
std::ostream& var_opt_sketch<T,S,A>::items_to_stream(std::ostream& os) const {
  os << "### Sketch Items" << std::endl;

  const uint32_t print_length = (n_ < k_ ? n_ : k_ + 1);
  for (int i = 0; i < print_length; ++i) {
    if (i == h_) {
      os << i << ": GAP" << std::endl;
    } else {
      os << i << ": " << data_[i] << "\twt = " << weights_[i] << std::endl;
    }
  }

  return os;
}

template <typename T, typename S, typename A>
std::string var_opt_sketch<T,S,A>::to_string() const {
  std::ostringstream ss;
  to_stream(ss);
  return ss.str();
}

template <typename T, typename S, typename A>
std::string var_opt_sketch<T,S,A>::items_to_string() const {
  std::ostringstream ss;
  items_to_stream(ss);
  return ss.str();
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::update(const T& item, double weight, bool mark) {
  if (weight < 0.0 || std::isnan(weight) || std::isinf(weight)) {
    throw std::invalid_argument("Item weights must be nonnegativge and finite. Found: "
                                + std::to_string(weight));
  } else if (weight == 0.0) {
    return;
  }
  ++n_;

  if (r_ == 0) {
    // exact mode
    update_warmup_phase(item, weight, mark);
  } else {
    // sketch is in estimation mode so we can make the following check
    assert(h_ == 0 || (peek_min() >= get_tau()));

    // what tau would be if deletion candidates turn out to be R plus the new item
    // note: (r_ + 1) - 1 is intentional
    const double hypothetical_tau = (weight + total_wt_r_) / ((r_ + 1) - 1);

    // is new item's turn to be considered for reservoir?
    const double condition1 = (h_ == 0) || (weight <= peek_min());

    // is new item light enough for reservoir?
    const double condition2 = weight < hypothetical_tau;
  
    if (condition1 && condition2) {
      update_light(item, weight, mark);
    } else if (r_ == 1) {
      update_heavy_r_eq1(item, weight, mark);
    } else {
      update_heavy_general(item, weight, mark);
    }
  }
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::update_warmup_phase(const T& item, double weight, bool mark) {
  assert(r_ == 0);
  assert(m_ == 0);
  assert(h_ <= k_);

  if (h_ >= curr_items_alloc_) {
    grow_data_arrays();
  }

  // store items as they come in until full
  A().construct(&data_[h_], T(item));
  weights_[h_] = weight;
  if (marks_ != nullptr) {
    marks_[h_] = mark;
  }
  ++h_;
  num_marks_in_h_ += mark ? 1 : 0;

  // check if need to heapify
  if (h_ > k_) {
    transition_from_warmup();
  }
}

/* In the "light" case the new item has weight <= old_tau, so
   would appear to the right of the R items in a hypothetical reverse-sorted
   list. It is easy to prove that it is light enough to be part of this
   round's downsampling */
template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::update_light(const T& item, double weight, bool mark) {
  assert(r_ >= 1);
  assert((r_ + h_) == k_);

  const int m_slot = h_; // index of the gap, which becomes the M region
  if (filled_data_) {
    data_[m_slot] = item;
  } else {
    A().construct(&data_[m_slot], T(item));
    filled_data_ = true;
  }
  weights_[m_slot] = weight;
  if (marks_ != nullptr) { marks_[m_slot] = mark; }
  ++m_;
  
  grow_candidate_set(total_wt_r_ + weight, r_ + 1);
}

/* In the "heavy" case the new item has weight > old_tau, so would
   appear to the left of items in R in a hypothetical reverse-sorted list and
   might or might not be light enough be part of this round's downsampling.
   [After first splitting off the R=1 case] we greatly simplify the code by
   putting the new item into the H heap whether it needs to be there or not.
   In other words, it might go into the heap and then come right back out,
   but that should be okay because pseudo_heavy items cannot predominate
   in long streams unless (max wt) / (min wt) > o(exp(N)) */
template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::update_heavy_general(const T& item, double weight, bool mark) {
  assert(m_ == 0);
  assert(r_ >= 2);
  assert((r_ + h_) == k_);

  // put into H, although may come back out momentarily
  push(item, weight, mark);

  grow_candidate_set(total_wt_r_, r_);
}

/* The analysis of this case is similar to that of the general heavy case.
   The one small technical difference is that since R < 2, we must grab an M item
   to have a valid starting point for continue_by_growing_candidate_set () */
template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::update_heavy_r_eq1(const T& item, double weight, bool mark) {
  assert(m_ == 0);
  assert(r_ == 1);
  assert((r_ + h_) == k_);

  push(item, weight, mark);  // new item into H
  pop_min_to_m_region();     // pop lightest back into M

  // Any set of two items is downsample-able to one item,
  // so the two lightest items are a valid starting point for the following
  const int m_slot = k_ - 1; // array is k+1, 1 in R, so slot before is M
  grow_candidate_set(weights_[m_slot] + total_wt_r_, 2);
}

/**
 * Decreases sketch's value of k by 1, updating stored values as needed.
 *
 * <p>Subject to certain pre-conditions, decreasing k causes tau to increase. This fact is used by
 * the unioning algorithm to force "marked" items out of H and into the reservoir region.</p>
 */
template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::decrease_k_by_1() {
  if (k_ <= 1) {
    throw std::logic_error("Cannot decrease k below 1 in union");
  }

  if ((h_ == 0) && (r_ == 0)) {
    // exact mode, but no data yet; this reduction is somewhat gratuitous
    --k_;
  } else if ((h_ > 0) && (r_ == 0)) {
    // exact mode, but we have some data
    --k_;
    if (h_ > k_) {
      transition_from_warmup();
    }
  } else if ((h_ > 0) && (r_ > 0)) {
    // reservoir mode, but we have some exact samples.
    // Our strategy will be to pull an item out of H (which we are allowed to do since it's
    // still just data), reduce k, and then re-insert the item

    // first, slide the R zone to the left by 1, temporarily filling the gap
    const uint32_t old_gap_idx = h_;
    const uint32_t old_final_r_idx = (h_ + 1 + r_) - 1;

    assert(old_final_r_idx == k_);
    swap_values(old_final_r_idx, old_gap_idx);

    // now we pull an item out of H; any item is ok, but if we grab the rightmost and then
    // reduce h_, the heap invariant will be preserved (and the gap will be restored), plus
    // the push() of the item that will probably happen later will be cheap.

    const uint32_t pulled_idx = h_ - 1;
    double pulled_weight = weights_[pulled_idx];
    bool pulled_mark = marks_[pulled_idx];
    // will move the pulled item below; don't do antying to it here

    if (pulled_mark) { --num_marks_in_h_; }
    weights_[pulled_idx] = -1.0; // to make bugs easier to spot

    --h_;
    --k_;
    --n_; // will be re-incremented with the update

    update(std::move(data_[pulled_idx]), pulled_weight, pulled_mark);
  } else if ((h_ == 0) && (r_ > 0)) {
    // pure reservoir mode, so can simply eject a randomly chosen sample from the reservoir
    assert(r_ >= 2);

    const uint32_t r_idx_to_delete = 1 + next_int(r_); // 1 for the gap
    const uint32_t rightmost_r_idx = (1 + r_) - 1;
    swap_values(r_idx_to_delete, rightmost_r_idx);
    weights_[rightmost_r_idx] = -1.0;

    --k_;
    --r_;
  }
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::allocate_data_arrays(uint32_t tgt_size, bool use_marks) {
  filled_data_ = false;

  data_ = A().allocate(tgt_size);
  weights_ = AllocDouble().allocate(tgt_size);

  if (use_marks) {
    marks_ = AllocBool().allocate(tgt_size);
  } else {
    marks_ = nullptr;
  }
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::grow_data_arrays() {
  const uint32_t prev_size = curr_items_alloc_;
  curr_items_alloc_ = get_adjusted_size(k_, curr_items_alloc_ << rf_);
  if (curr_items_alloc_ == k_) {
    ++curr_items_alloc_;
  }

  if (prev_size < curr_items_alloc_) {
    filled_data_ = false;

    T* tmp_data = A().allocate(curr_items_alloc_);
    double* tmp_weights = AllocDouble().allocate(curr_items_alloc_);

    for (int i = 0; i < prev_size; ++i) {
      A().construct(&tmp_data[i], std::move(data_[i]));
      A().destroy(data_ + i);
      tmp_weights[i] = weights_[i];
    }

    A().deallocate(data_, prev_size);
    AllocDouble().deallocate(weights_, prev_size);

    data_ = tmp_data;
    weights_ = tmp_weights;

    if (marks_ != nullptr) {
      bool* tmp_marks = AllocBool().allocate(curr_items_alloc_);
      for (int i = 0; i < prev_size; ++i) {
        tmp_marks[i] = marks_[i];
      }
      AllocBool().deallocate(marks_, prev_size);
      marks_ = tmp_marks;
    }
  }
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::transition_from_warmup() {
  // Move the 2 lightest items from H to M
  // But the lighter really belongs in R, so update counts to reflect that
  convert_to_heap();
  pop_min_to_m_region();
  pop_min_to_m_region();
  --m_;
  ++r_;

  assert(h_ == (k_ - 1));
  assert(m_ == 1);
  assert(r_ == 1);

  // Update total weight in R and then, having grabbed the value, overwrite
  // in weight_ array to help make bugs more obvious
  total_wt_r_ = weights_[k_]; // only one item, known location
  weights_[k_] = -1.0;

  // The two lightest items are ncessarily downsample-able to one item,
  // and are therefore a valid initial candidate set
  grow_candidate_set(weights_[k_ - 1] + total_wt_r_, 2);
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::convert_to_heap() {
  if (h_ < 2) {
    return; // nothing to do
  }

  const int last_slot = h_ - 1;
  const int last_non_leaf = ((last_slot + 1) / 2) - 1;
  
  for (int j = last_non_leaf; j >= 0; --j) {
    restore_towards_leaves(j);
  }

  // validates heap, used for initial debugging
  //for (int j = h_ - 1; j >= 1; --j) {
  //  int p = ((j + 1) / 2) - 1;
  //  assert(weights_[p] <= weights_[j]);
  //}
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::restore_towards_leaves(int slot_in) {
  assert(h_ > 0);
  const int last_slot = h_ - 1;
  assert(slot_in <= last_slot);

  int slot = slot_in;
  int child = (2 * slot_in) + 1; // might be invalid, need to check

  while (child <= last_slot) {
    int child2 = child + 1; // might also be invalid
    if ((child2 <= last_slot) && (weights_[child2] < weights_[child])) {
      // siwtch to other child if it's both valid and smaller
      child = child2;
    }

    if (weights_[slot] <= weights_[child]) {
      // invariant holds so we're done
      break;
    }

    // swap and continue
    swap_values(slot, child);

    slot = child;
    child = (2 * slot) + 1; // might be invalid, checked on next loop
  }
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::restore_towards_root(int slot_in) {
  int slot = slot_in;
  int p = (((slot + 1) / 2) - 1); // valid if slot >= 1
  while ((slot > 0) && (weights_[slot] < weights_[p])) {
    swap_values(slot, p);
    slot = p;
    p = (((slot + 1) / 2) - 1); // valid if slot >= 1
  }
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::push(const T& item, double wt, bool mark) {
  if (filled_data_) {
    data_[h_] = item;
  } else {
    A().construct(&data_[h_], T(item));
    filled_data_ = true;
  }
  weights_[h_] = wt;
  if (marks_ != nullptr) {
    marks_[h_] = mark;
    num_marks_in_h_ += (mark ? 1 : 0);
  }
  ++h_;

  restore_towards_root(h_ - 1); // need use old h_, but want accurate h_
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::pop_min_to_m_region() {
  assert(h_ > 0);
  assert(h_ + m_ + r_ == k_ + 1);

  if (h_ == 1) {
    // just update bookkeeping
    ++m_;
    --h_;
  } else {
    // main case
    int tgt = h_ - 1; // last slot, will swap with root
    swap_values(0, tgt);
    ++m_;
    --h_;

    restore_towards_leaves(0);
  }

  if (is_marked(h_)) {
    --num_marks_in_h_;
  }
}


template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::swap_values(int src, int dst) {
  std::swap(data_[src], data_[dst]);
  std::swap(weights_[src], weights_[dst]);

  if (marks_ != nullptr) {
    std::swap(marks_[src], marks_[dst]);
  }
}

/* When entering here we should be in a well-characterized state where the
   new item has been placed in either h or m and we have a valid but not necessarily
   maximal sampling plan figured out. The array is completely full at this point.
   Everyone in h and m has an explicit weight. The candidates are right-justified
   and are either just the r set or the r set + exactly one m item. The number
   of cands is at least 2. We will now grow the candidate set as much as possible
   by pulling sufficiently light items from h to m.
*/
template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::grow_candidate_set(double wt_cands, int num_cands) {
  assert(h_ + m_ + r_ == k_ + 1);
  assert(num_cands >= 2);
  assert(num_cands == m_ + r_);
  assert(m_ == 0 || m_ == 1);

  while (h_ > 0) {
    const double next_wt = peek_min();
    const double next_tot_wt = wt_cands + next_wt;

    // test for strict lightness of next prospect (denominator multiplied through)
    // ideally: (next_wt * (next_num_cands-1) < next_tot_wt)
    //          but can use num_cands directly
    if ((next_wt * num_cands) < next_tot_wt) {
      wt_cands = next_tot_wt;
      ++num_cands;
      pop_min_to_m_region(); // adjusts h_ and m_
    } else {
      break;
    }
  }

  downsample_candidate_set(wt_cands, num_cands);
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::downsample_candidate_set(double wt_cands, int num_cands) {
  assert(num_cands >= 2);
  assert(h_ + num_cands == k_ + 1);

  // need this before overwriting anything
  const int delete_slot = choose_delete_slot(wt_cands, num_cands);
  const int leftmost_cand_slot = h_;
  assert(delete_slot >= leftmost_cand_slot);
  assert(delete_slot <= k_);

  // Overwrite weights for items from M moving into R,
  // to make bugs more obvious. Also needed so anyone reading the
  // weight knows if it's invalid without checking h_ and m_
  const int stop_idx = leftmost_cand_slot + m_;
  for (int j = leftmost_cand_slot; j < stop_idx; ++j) {
    weights_[j] = -1.0;
  }

  // The next two lines work even when delete_slot == leftmost_cand_slot
  data_[delete_slot] = std::move(data_[leftmost_cand_slot]);
  // cannot set data_[leftmost_cand_slot] to null since not uisng T*

  m_ = 0;
  r_ = num_cands - 1;
  total_wt_r_ = wt_cands;
}

template<typename T, typename S, typename A>
uint32_t var_opt_sketch<T,S,A>::choose_delete_slot(double wt_cands, int num_cands) const {
  assert(r_ > 0);

  if (m_ == 0) {
    // this happens if we insert a really have item
    return pick_random_slot_in_r();
  } else if (m_ == 1) {
    // check if we keep th item in M or pick oen from R
    // p(keep) = (num_cand - 1) * wt_M / wt_cand
    double wt_m_cand = weights_[h_]; // slot of item in M is h_
    if ((wt_cands * next_double_exclude_zero()) < ((num_cands - 1) * wt_m_cand)) {
      return pick_random_slot_in_r(); // keep item in M
    } else {
      return h_; // indext of item in M
    }
  } else {
    // general case
    const int delete_slot = choose_weighted_delete_slot(wt_cands, num_cands);
    const int first_r_slot = h_ + m_;
    if (delete_slot == first_r_slot) {
      return pick_random_slot_in_r();
    } else {
      return delete_slot;
    }
  }
}

template<typename T, typename S, typename A>
uint32_t var_opt_sketch<T,S,A>::choose_weighted_delete_slot(double wt_cands, int num_cands) const {
  assert(m_ >= 1);

  const int offset = h_;
  const int final_m = (offset + m_) - 1;
  const int num_to_keep = num_cands - 1;

  double left_subtotal = 0.0;
  double right_subtotal = -1.0 * wt_cands * next_double_exclude_zero();

  for (int i = offset; i <= final_m; ++i) {
    left_subtotal += num_to_keep * weights_[i];
    right_subtotal += wt_cands;

    if (left_subtotal < right_subtotal) {
      return i;
    }
  }

  // this slot tells caller that we need to delete out of R
  return final_m + 1;
}

template<typename T, typename S, typename A>
uint32_t var_opt_sketch<T,S,A>::pick_random_slot_in_r() const {
  assert(r_ > 0);
  const int offset = h_ + m_;
  if (r_ == 1) {
    return offset;
  } else {
    return offset + next_int(r_);
  }
}

template<typename T, typename S, typename A>
double var_opt_sketch<T,S,A>::peek_min() const {
  assert(h_ > 0);
  return weights_[0];
}

template<typename T, typename S, typename A>
inline bool var_opt_sketch<T,S,A>::is_marked(int idx) const {
  return marks_ == nullptr ? false : marks_[idx];
}

template<typename T, typename S, typename A>
double var_opt_sketch<T,S,A>::get_tau() const {
  return r_ == 0 ? std::nan("1") : (total_wt_r_ / r_);
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::strip_marks() {
  assert(marks_ != nullptr);
  num_marks_in_h_ = 0;
  AllocBool().deallocate(marks_, curr_items_alloc_);
  marks_ = nullptr;
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::check_preamble_longs(uint8_t preamble_longs, uint8_t flags) {
  const bool is_empty(flags & EMPTY_FLAG_MASK);
  
  if (is_empty) {
    if (preamble_longs != PREAMBLE_LONGS_EMPTY) {
      throw std::invalid_argument("Possible corruption: Preamble longs must be "
        + std::to_string(PREAMBLE_LONGS_EMPTY) + " for an empty sketch. Found: "
        + std::to_string(preamble_longs));
    }
  } else {
    if (preamble_longs != PREAMBLE_LONGS_WARMUP
        && preamble_longs != PREAMBLE_LONGS_FULL) {
      throw std::invalid_argument("Possible corruption: Preamble longs must be "
        + std::to_string(PREAMBLE_LONGS_WARMUP) + " or "
        + std::to_string(PREAMBLE_LONGS_FULL)
        + " for a non-empty sketch. Found: " + std::to_string(preamble_longs));
    }
  }
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::check_family_and_serialization_version(uint8_t family_id, uint8_t ser_ver) {
  if (family_id == FAMILY_ID) {
    if (ser_ver != SER_VER) {
      throw std::invalid_argument("Possible corruption: VarOpt serialization version must be "
        + std::to_string(SER_VER) + ". Found: " + std::to_string(ser_ver));
    }
    return;
  }
  // TODO: extend to handle reservoir sampling

  throw std::invalid_argument("Possible corruption: VarOpt family id must be "
    + std::to_string(FAMILY_ID) + ". Found: " + std::to_string(family_id));
}

template<typename T, typename S, typename A>
void var_opt_sketch<T, S, A>::validate_and_set_current_size(int preamble_longs) {
  if (k_ == 0 || k_ > MAX_K) {
    throw std::invalid_argument("k must be at least 1 and less than 2^31 - 1");
  }

  if (n_ <= k_) {
    if (preamble_longs != PREAMBLE_LONGS_WARMUP) {
      throw std::invalid_argument("Possible corruption: deserializing with n <= k but not in warmup mode. "
       "Found n = " + std::to_string(n_) + ", k = " + std::to_string(k_));
    }
    if (n_ != h_) {
      throw std::invalid_argument("Possible corruption: deserializing in warmup mode but n != h. "
       "Found n = " + std::to_string(n_) + ", h = " + std::to_string(h_));
    }
    if (r_ > 0) {
      throw std::invalid_argument("Possible corruption: deserializing in warmup mode but r > 0. "
       "Found r = " + std::to_string(r_));
    }

    const uint32_t ceiling_lg_k = to_log_2(ceiling_power_of_2(k_));
    const uint32_t min_lg_size = to_log_2(ceiling_power_of_2(h_));
    const int initial_lg_size = starting_sub_multiple(ceiling_lg_k, rf_, min_lg_size);
    curr_items_alloc_ = get_adjusted_size(k_, 1 << initial_lg_size);
    if (curr_items_alloc_ == k_) { // if full size, need to leave 1 for the gap
      ++curr_items_alloc_;
    }
  } else { // n_ > k_
    if (preamble_longs != PREAMBLE_LONGS_FULL) { 
      throw std::invalid_argument("Possible corruption: deserializing with n > k but not in full mode. "
       "Found n = " + std::to_string(n_) + ", k = " + std::to_string(k_));
    }
    if (h_ + r_ != k_) {
      throw std::invalid_argument("Possible corruption: deserializing in full mode but h + r != n. "
       "Found h = " + std::to_string(h_) + ", r = " + std::to_string(r_) + ", n = " + std::to_string(n_));
    }

    curr_items_alloc_ = k_ + 1;
  }
}


template<typename T, typename S, typename A>
template<typename P>
subset_summary var_opt_sketch<T, S, A>::estimate_subset_sum(P predicate) const {
  if (n_ == 0) {
    return {0.0, 0.0, 0.0, 0.0};
  }

  double total_wt_h = 0.0;
  double h_true_wt = 0.0;
  size_t idx = 0;
  for (; idx < h_; ++idx) {
    double wt = weights_[idx];
    total_wt_h += wt;
    if (predicate(data_[idx])) {
      h_true_wt += wt;
    }
  }

  // if only heavy items, we have an exact answre
  if (r_ == 0) {
    return {h_true_wt, h_true_wt, h_true_wt, h_true_wt};
  }

  const uint64_t num_samples = n_ - h_;
  assert(num_samples > 0);
  double effective_sampling_rate = r_ / static_cast<double>(num_samples);
  assert(effective_sampling_rate >= 0.0);
  assert(effective_sampling_rate <= 1.0);

  size_t r_true_count = 0;
  ++idx; // skip the gap
  for (; idx < (k_ + 1); ++idx) {
    if (predicate(data_[idx])) {
      ++r_true_count;
    }
  }

  double lb_true_fraction = pseudo_hypergeometric_lb_on_p(r_, r_true_count, effective_sampling_rate);
  double estimated_true_fraction = (1.0 * r_true_count) / r_;
  double ub_true_fraction = pseudo_hypergeometric_ub_on_p(r_, r_true_count, effective_sampling_rate);

  return {  h_true_wt + (total_wt_r_ * lb_true_fraction),
            h_true_wt + (total_wt_r_ * estimated_true_fraction),
            h_true_wt + (total_wt_r_ * ub_true_fraction),
            total_wt_h + total_wt_r_
         };
}

template<typename T, typename S, typename A>
typename var_opt_sketch<T, S, A>::const_iterator var_opt_sketch<T, S, A>::begin() const {
  return var_opt_sketch<T, S, A>::const_iterator(*this, false);
}

template<typename T, typename S, typename A>
typename var_opt_sketch<T, S, A>::const_iterator var_opt_sketch<T, S, A>::end() const {
  return var_opt_sketch<T, S, A>::const_iterator(*this, true);
}

// -------- var_opt_sketch::const_iterator implementation ---------

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>::const_iterator::const_iterator(const var_opt_sketch<T,S,A>& sk, bool is_end) :
  sk_(&sk),
  cum_r_weight_(0.0),
  r_item_wt_(sk.get_tau()),
  final_idx_(sk.r_ > 0 ? sk.h_ + sk.r_ + 1 : sk.h_),
  weight_correction_(false)
{
  // index logic easier to read if not inline
  if (is_end) {
    idx_ = final_idx_;
    sk_ = nullptr;
  } else {
    idx_ = (sk.h_ == 0 && sk.r_ > 0 ? 1 : 0); // skip if gap is at start
  }

  // should only apply if sketch is empty
  if (idx_ == final_idx_) { sk_ = nullptr; }
}

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>::const_iterator::const_iterator(const var_opt_sketch<T,S,A>& sk, bool is_end, bool use_r_region, bool weight_corr) :
  sk_(&sk),
  cum_r_weight_(0.0),
  r_item_wt_(sk.get_tau()),
  final_idx_(sk.h_ + (use_r_region ? 1 + sk.r_ : 0)),
  weight_correction_(weight_corr)
{
  if (use_r_region) {
    idx_ = sk.h_ + 1 + (is_end ? sk.r_ : 0);
  } else { // H region
    // gap at start only if h_ == 0, so index always starts at 0
    idx_ = (is_end ? sk.h_ : 0);
  }
  
  // unlike in full iterator case, may happen even if sketch is not empty
  if (idx_ == final_idx_) { sk_ = nullptr; }
}


template<typename T,  typename S, typename A>
var_opt_sketch<T, S, A>::const_iterator::const_iterator(const const_iterator& other) :
  sk_(other.sk_),
  cum_r_weight_(other.cum_r_weight_),
  r_item_wt_(other.r_item_wt_),
  idx_(other.idx_),
  final_idx_(other.final_idx_),
  weight_correction_(other.weight_correction_)
{}

template<typename T,  typename S, typename A>
typename var_opt_sketch<T, S, A>::const_iterator& var_opt_sketch<T, S, A>::const_iterator::operator++() {
  ++idx_;
  
  if (idx_ == final_idx_) {
    sk_ = nullptr;
    return *this;
  } else if (idx_ == sk_->h_ && sk_->r_ > 0) { // check for the gap
    ++idx_;
  }
  if (idx_ > sk_->h_) { cum_r_weight_ += r_item_wt_; }
  return *this;
}

template<typename T,  typename S, typename A>
typename var_opt_sketch<T, S, A>::const_iterator& var_opt_sketch<T, S, A>::const_iterator::operator++(int) {
  const_iterator tmp(*this);
  operator++();
  return tmp;
}

template<typename T, typename S, typename A>
bool var_opt_sketch<T, S, A>::const_iterator::operator==(const const_iterator& other) const {
  if (sk_ != other.sk_) return false;
  if (sk_ == nullptr) return true; // end (and we know other.sk_ is also null)
  return idx_ == other.idx_;
}

template<typename T, typename S, typename A>
bool var_opt_sketch<T, S, A>::const_iterator::operator!=(const const_iterator& other) const {
  return !operator==(other);
}

template<typename T, typename S, typename A>
const std::pair<const T&, const double> var_opt_sketch<T, S, A>::const_iterator::operator*() const {
  double wt;
  if (idx_ < sk_->h_) {
    wt = sk_->weights_[idx_];
  } else if (weight_correction_ && idx_ == final_idx_ - 1) {
    wt = sk_->total_wt_r_ - cum_r_weight_;
  } else {
    wt = r_item_wt_;
  }
  return std::pair<const T&, const double>(sk_->data_[idx_], wt);
}

template<typename T, typename S, typename A>
const bool var_opt_sketch<T, S, A>::const_iterator::get_mark() const {
  return sk_->marks_ == nullptr ? false : sk_->marks_[idx_];
}


// ******************** MOVE TO COMMON UTILS AREA EVENTUALLY *********************

namespace random_utils {
  static std::random_device rd; // possibly unsafe in MinGW with GCC < 9.2
  static std::mt19937_64 rand(rd());
  static std::uniform_real_distribution<> next_double(0.0, 1.0);
}

/**
 * Checks if target sampling allocation is more than 50% of max sampling size.
 * If so, returns max sampling size, otherwise passes through target size.
 */
template<typename T, typename S, typename A>
uint32_t var_opt_sketch<T,S,A>::get_adjusted_size(int max_size, int resize_target) {
  if (max_size - (resize_target << 1) < 0L) {
    return max_size;
  }
  return resize_target;
}

template<typename T, typename S, typename A>
uint32_t var_opt_sketch<T,S,A>::starting_sub_multiple(int lg_target, int lg_rf, int lg_min) {
  return (lg_target <= lg_min)
          ? lg_min : (lg_rf == 0) ? lg_target
          : (lg_target - lg_min) % lg_rf + lg_min;
}

template<typename T, typename S, typename A>
double var_opt_sketch<T,S,A>::pseudo_hypergeometric_ub_on_p(uint64_t n, uint32_t k, double sampling_rate) {
  const double adjusted_kappa = DEFAULT_KAPPA * sqrt(1 - sampling_rate);
  return bounds_binomial_proportions::approximate_upper_bound_on_p(n, k, adjusted_kappa);
}

template<typename T, typename S, typename A>
double var_opt_sketch<T,S,A>::pseudo_hypergeometric_lb_on_p(uint64_t n, uint32_t k, double sampling_rate) {
  const double adjusted_kappa = DEFAULT_KAPPA * sqrt(1 - sampling_rate);
  return bounds_binomial_proportions::approximate_lower_bound_on_p(n, k, adjusted_kappa);
}

template<typename T, typename S, typename A>
bool var_opt_sketch<T,S,A>::is_power_of_2(uint32_t v) {
  return v && !(v & (v - 1));
}

template<typename T, typename S, typename A>
uint32_t var_opt_sketch<T,S,A>::to_log_2(uint32_t v) {
  if (is_power_of_2(v)) {
    return count_trailing_zeros(v);
  } else {
    throw std::invalid_argument("Attempt to compute integer log2 of non-positive or non-power of 2");
  }
}

// Returns an integer in the range [0, max_value) -- excludes max_value
template<typename T, typename S, typename A>
uint32_t var_opt_sketch<T,S,A>::next_int(uint32_t max_value) {
  std::uniform_int_distribution<uint32_t> dist(0, max_value - 1);
  return dist(random_utils::rand);
}

template<typename T, typename S, typename A>
double var_opt_sketch<T,S,A>::next_double_exclude_zero() {
  double r = random_utils::next_double(random_utils::rand);
  while (r == 0.0) {
    r = random_utils::next_double(random_utils::rand);
  }
  return r;
}

template<typename T, typename S, typename A>
uint32_t var_opt_sketch<T,S,A>::count_trailing_zeros(uint32_t v) {
  static const uint8_t ctz_byte_count[256] = {8,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,5,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,6,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,5,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,7,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,5,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,6,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,5,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0};

  uint32_t tmp = v;
  for (int j = 0; j < 4; ++j) {
    const int byte = (tmp & 0xFFUL);
    if (byte != 0) return (j << 3) + ctz_byte_count[byte];
    tmp >>= 8;
  }
  return 64;
}

template<typename T, typename S, typename A>
uint32_t var_opt_sketch<T,S,A>::ceiling_power_of_2(uint32_t n) {
  --n;
  n |= n >> 1;
  n |= n >> 2;
  n |= n >> 4;
  n |= n >> 8;
  n |= n >> 16;
  return ++n;
}

}

// namespace datasketches

#endif // _VAR_OPT_SKETCH_IMPL_HPP_
