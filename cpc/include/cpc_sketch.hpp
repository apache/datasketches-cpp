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

#ifndef CPC_SKETCH_HPP_
#define CPC_SKETCH_HPP_

#include <iostream>
#include <memory>
#include <functional>
#include <stdexcept>
#include <cmath>

#include "fm85.h"
#include "fm85Compression.h"
#include "iconEstimator.h"
#include "fm85Confidence.h"
#include "fm85Util.h"
#include "MurmurHash3.h"
#include "cpc_common.hpp"

namespace datasketches {

/*
 * High performance C++ implementation of Compressed Probabilistic Counting sketch
 *
 * author Kevin Lang
 * author Alexander Saydakov
 */

typedef std::unique_ptr<void, std::function<void(void*)>> ptr_with_deleter;

class cpc_sketch;
typedef std::unique_ptr<cpc_sketch, void(*)(cpc_sketch*)> cpc_sketch_unique_ptr;

// allocation and initialization of global compression tables
// call this before anything else if you want to control the initialization time
// or you want to use a custom memory allocation and deallocation mechanism
// otherwise initialization happens during instantiation of the first cpc_sketch or cpc_union
// it is safe to call more than once assuming no race conditions
// this is not thread safe! neither is the rest of the library
void cpc_init(void* (*alloc)(size_t) = &malloc, void (*dealloc)(void*) = &free);

// optional deallocation of globally allocated compression tables
void cpc_cleanup();

class cpc_sketch {
  public:

    explicit cpc_sketch(uint8_t lg_k = CPC_DEFAULT_LG_K, uint64_t seed = DEFAULT_SEED) : seed(seed) {
      fm85Init();
      if (lg_k < CPC_MIN_LG_K or lg_k > CPC_MAX_LG_K) {
        throw std::invalid_argument("lg_k must be >= " + std::to_string(CPC_MIN_LG_K) + " and <= " + std::to_string(CPC_MAX_LG_K) + ": " + std::to_string(lg_k));
      }
      state = fm85Make(lg_k);
    }

    cpc_sketch(const cpc_sketch& other) : state(fm85Copy(other.state)), seed(other.seed) {}

    cpc_sketch& operator=(cpc_sketch other) {
      seed = other.seed;
      std::swap(state, other.state); // @suppress("Invalid arguments")
      return *this;
    }

    ~cpc_sketch() {
      fm85Free(state);
    }

    bool is_empty() const {
      return state->numCoupons == 0;
    }

    double get_estimate() const {
      if (!state->mergeFlag) return getHIPEstimate(state);
      return getIconEstimate(state->lgK, state->numCoupons);
    }

    double get_lower_bound(unsigned kappa) const {
      if (kappa > 3) {
        throw std::invalid_argument("kappa must be 1, 2 or 3");
      }
      if (!state->mergeFlag) return getHIPConfidenceLB(state, kappa);
      return getIconConfidenceLB(state, kappa);
    }

    double get_upper_bound(unsigned kappa) const {
      if (kappa > 3) {
        throw std::invalid_argument("kappa must be 1, 2 or 3");
      }
      if (!state->mergeFlag) return getHIPConfidenceUB(state, kappa);
      return getIconConfidenceUB(state, kappa);
    }

    void update(const std::string& value) {
      if (value.empty()) return;
      update(value.c_str(), value.length());
    }

    void update(uint64_t value) {
      update(&value, sizeof(value));
    }

    void update(int64_t value) {
      update(&value, sizeof(value));
    }

    // for compatibility with Java implementation
    void update(uint32_t value) {
      update(static_cast<int32_t>(value));
    }

    // for compatibility with Java implementation
    void update(int32_t value) {
      update(static_cast<int64_t>(value));
    }

    // for compatibility with Java implementation
    void update(uint16_t value) {
      update(static_cast<int16_t>(value));
    }

    // for compatibility with Java implementation
    void update(int16_t value) {
      update(static_cast<int64_t>(value));
    }

    // for compatibility with Java implementation
    void update(uint8_t value) {
      update(static_cast<int8_t>(value));
    }

    // for compatibility with Java implementation
    void update(int8_t value) {
      update(static_cast<int64_t>(value));
    }

    typedef union {
      int64_t long_value;
      double double_value;
    } long_double_union;

    // for compatibility with Java implementation
    void update(double value) {
      long_double_union ldu;
      if (value == 0.0) {
        ldu.double_value = 0.0; // canonicalize -0.0 to 0.0
      } else if (std::isnan(value)) {
        ldu.long_value = 0x7ff8000000000000L; // canonicalize NaN using value from Java's Double.doubleToLongBits()
      } else {
        ldu.double_value = value;
      }
      update(&ldu, sizeof(ldu));
    }

    // for compatibility with Java implementation
    void update(float value) {
      update(static_cast<double>(value));
    }

    // Be very careful to hash input values consistently using the same approach
    // either over time or on different platforms
    // or while passing sketches from Java environment or to Java environment
    // Otherwise two sketches that should represent overlapping sets will be disjoint
    // For instance, for signed 32-bit values call update(int32_t) method above,
    // which does widening conversion to int64_t, if compatibility with Java is expected
    void update(const void* value, int size) {
      HashState hashes;
      MurmurHash3_x64_128(value, size, seed, hashes);
      fm85Update(state, hashes.h1, hashes.h2);
    }

    void serialize(std::ostream& os) const {
      FM85* compressed = fm85Compress(state);
      const uint8_t preamble_ints(get_preamble_ints(compressed));
      os.write((char*)&preamble_ints, sizeof(preamble_ints));
      const uint8_t serial_version(SERIAL_VERSION);
      os.write((char*)&serial_version, sizeof(serial_version));
      const uint8_t family(FAMILY);
      os.write((char*)&family, sizeof(family));
      const uint8_t lg_k(compressed->lgK);
      os.write((char*)&lg_k, sizeof(lg_k));
      const uint8_t first_interesting_column(compressed->firstInterestingColumn);
      os.write((char*)&first_interesting_column, sizeof(first_interesting_column));
      const bool has_hip(!compressed->mergeFlag);
      const bool has_table(compressed->compressedSurprisingValues != nullptr);
      const bool has_window(compressed->compressedWindow != nullptr);
      const uint8_t flags_byte(
        (1 << flags::IS_COMPRESSED)
        | (has_hip ? 1 << flags::HAS_HIP : 0)
        | (has_table ? 1 << flags::HAS_TABLE : 0)
        | (has_window ? 1 << flags::HAS_WINDOW : 0)
      );
      os.write((char*)&flags_byte, sizeof(flags_byte));
      const uint16_t seed_hash(compute_seed_hash(seed));
      os.write((char*)&seed_hash, sizeof(seed_hash));
      if (!is_empty()) {
        const uint32_t num_coupons(compressed->numCoupons);
        os.write((char*)&num_coupons, sizeof(num_coupons));
        if (has_table && has_window) {
          // if there is no window it is the same as number of coupons
          const uint32_t num_values(compressed->numCompressedSurprisingValues);
          os.write((char*)&num_values, sizeof(num_values));
          // HIP values are at the same offset because of alignment, which can be in two different places in the sequence of fields
          // this is the first HIP decision point
          if (has_hip) write_hip(compressed, os);
        }
        if (has_table) {
          const uint32_t csv_length(compressed->csvLength);
          os.write((char*)&csv_length, sizeof(csv_length));
        }
        if (has_window) {
          const uint32_t cw_length(compressed->cwLength);
          os.write((char*)&cw_length, sizeof(cw_length));
        }
        // this is the second HIP decision point
        if (has_hip && !(has_table && has_window)) write_hip(compressed, os);
        if (has_window) {
          os.write((char*)compressed->compressedWindow, compressed->cwLength * sizeof(uint32_t));
        }
        if (has_table) {
          os.write((char*)compressed->compressedSurprisingValues, compressed->csvLength * sizeof(uint32_t));
        }
      }
      fm85Free(compressed);
    }

    std::pair<ptr_with_deleter, const size_t> serialize(unsigned header_size_bytes = 0) const {
      FM85* compressed = fm85Compress(state);
      const uint8_t preamble_ints(get_preamble_ints(compressed));
      const size_t size = header_size_bytes + (preamble_ints + compressed->csvLength + compressed->cwLength) * sizeof(uint32_t);
      ptr_with_deleter data_ptr(
          fm85alloc(size),
          [](void* ptr) { fm85free(ptr); }
      );
      char* ptr = static_cast<char*>(data_ptr.get()) + header_size_bytes;
      ptr += copy_to_mem(ptr, &preamble_ints, sizeof(preamble_ints));
      const uint8_t serial_version(SERIAL_VERSION);
      ptr += copy_to_mem(ptr, &serial_version, sizeof(serial_version));
      const uint8_t family(FAMILY);
      ptr += copy_to_mem(ptr, &family, sizeof(family));
      const uint8_t lg_k(compressed->lgK);
      ptr += copy_to_mem(ptr, &lg_k, sizeof(lg_k));
      const uint8_t first_interesting_column(compressed->firstInterestingColumn);
      ptr += copy_to_mem(ptr, &first_interesting_column, sizeof(first_interesting_column));
      const bool has_hip(!compressed->mergeFlag);
      const bool has_table(compressed->compressedSurprisingValues != nullptr);
      const bool has_window(compressed->compressedWindow != nullptr);
      const uint8_t flags_byte(
        (1 << flags::IS_COMPRESSED)
        | (has_hip ? 1 << flags::HAS_HIP : 0)
        | (has_table ? 1 << flags::HAS_TABLE : 0)
        | (has_window ? 1 << flags::HAS_WINDOW : 0)
      );
      ptr += copy_to_mem(ptr, &flags_byte, sizeof(flags_byte));
      const uint16_t seed_hash(compute_seed_hash(seed));
      ptr += copy_to_mem(ptr, &seed_hash, sizeof(seed_hash));
      if (!is_empty()) {
        const uint32_t num_coupons(compressed->numCoupons);
        ptr += copy_to_mem(ptr, &num_coupons, sizeof(num_coupons));
        if (has_table && has_window) {
          // if there is no window it is the same as number of coupons
          const uint32_t num_values(compressed->numCompressedSurprisingValues);
          ptr += copy_to_mem(ptr, &num_values, sizeof(num_values));
          // HIP values are at the same offset because of alignment, which can be in two different places in the sequence of fields
          // this is the first HIP decision point
          if (has_hip) ptr += copy_hip_to_mem(compressed, ptr);
        }
        if (has_table) {
          const uint32_t csv_length(compressed->csvLength);
          ptr += copy_to_mem(ptr, &csv_length, sizeof(csv_length));
        }
        if (has_window) {
          const uint32_t cw_length(compressed->cwLength);
          ptr += copy_to_mem(ptr, &cw_length, sizeof(cw_length));
        }
        // this is the second HIP decision point
        if (has_hip && !(has_table && has_window)) ptr += copy_hip_to_mem(compressed, ptr);
        if (has_window) {
          ptr += copy_to_mem(ptr, compressed->compressedWindow, compressed->cwLength * sizeof(uint32_t));
        }
        if (has_table) {
          ptr += copy_to_mem(ptr, compressed->compressedSurprisingValues, compressed->csvLength * sizeof(uint32_t));
        }
      }
      if (ptr != static_cast<char*>(data_ptr.get()) + size) throw std::logic_error("serialized size mismatch");
      fm85Free(compressed);
      return std::make_pair(std::move(data_ptr), size);
    }

    static cpc_sketch_unique_ptr
    deserialize(std::istream& is, uint64_t seed = DEFAULT_SEED) {
      fm85Init();
      uint8_t preamble_ints;
      is.read((char*)&preamble_ints, sizeof(preamble_ints));
      uint8_t serial_version;
      is.read((char*)&serial_version, sizeof(serial_version));
      uint8_t family_id;
      is.read((char*)&family_id, sizeof(family_id));
      uint8_t lg_k;
      is.read((char*)&lg_k, sizeof(lg_k));
      uint8_t first_interesting_column;
      is.read((char*)&first_interesting_column, sizeof(first_interesting_column));
      uint8_t flags_byte;
      is.read((char*)&flags_byte, sizeof(flags_byte));
      uint16_t seed_hash;
      is.read((char*)&seed_hash, sizeof(seed_hash));
      const bool has_hip(flags_byte & (1 << flags::HAS_HIP));
      const bool has_table(flags_byte & (1 << flags::HAS_TABLE));
      const bool has_window(flags_byte & (1 << flags::HAS_WINDOW));
      FM85 compressed;
      compressed.isCompressed = 1;
      compressed.mergeFlag = has_hip ? 0 : 1;
      compressed.lgK = lg_k;
      compressed.firstInterestingColumn = first_interesting_column;
      compressed.numCoupons = 0;
      compressed.numCompressedSurprisingValues = 0;
      compressed.kxp = 1 << lg_k;
      compressed.hipEstAccum = 0;
      compressed.hipErrAccum = 0;
      compressed.csvLength = 0;
      compressed.cwLength = 0;
      compressed.compressedSurprisingValues = nullptr;
      compressed.compressedWindow = nullptr;
      compressed.surprisingValueTable = nullptr;
      compressed.slidingWindow = nullptr;
      if (has_table || has_window) {
        uint32_t num_coupons;
        is.read((char*)&num_coupons, sizeof(num_coupons));
        compressed.numCoupons = num_coupons;
        if (has_table && has_window) {
          uint32_t num_values;
          is.read((char*)&num_values, sizeof(num_values));
          compressed.numCompressedSurprisingValues = num_values;
          if (has_hip) read_hip(&compressed, is);
        }
        if (has_table) {
          uint32_t csv_length;
          is.read((char*)&csv_length, sizeof(csv_length));
          compressed.csvLength = csv_length;
        }
        if (has_window) {
          uint32_t cw_length;
          is.read((char*)&cw_length, sizeof(cw_length));
          compressed.cwLength = cw_length;
        }
        if (has_hip && !(has_table && has_window)) read_hip(&compressed, is);
        if (has_window) {
          compressed.compressedWindow = new uint32_t[compressed.cwLength];
          is.read((char*)compressed.compressedWindow, compressed.cwLength * sizeof(uint32_t));
        }
        if (has_table) {
          compressed.compressedSurprisingValues = new uint32_t[compressed.csvLength];
          is.read((char*)compressed.compressedSurprisingValues, compressed.csvLength * sizeof(uint32_t));
        }
        if (!has_window) compressed.numCompressedSurprisingValues = compressed.numCoupons;
      }
      compressed.windowOffset = determineCorrectOffset(compressed.lgK, compressed.numCoupons);

      uint8_t expected_preamble_ints(get_preamble_ints(&compressed));
      if (preamble_ints != expected_preamble_ints) {
        throw std::invalid_argument("Possible corruption: preamble ints: expected "
            + std::to_string(expected_preamble_ints) + ", got " + std::to_string(preamble_ints));
      }
      if (serial_version != SERIAL_VERSION) {
        throw std::invalid_argument("Possible corruption: serial version: expected "
            + std::to_string(SERIAL_VERSION) + ", got " + std::to_string(serial_version));
      }
      if (family_id != FAMILY) {
        throw std::invalid_argument("Possible corruption: family: expected "
            + std::to_string(FAMILY) + ", got " + std::to_string(family_id));
      }
      if (seed_hash != compute_seed_hash(seed)) {
        throw std::invalid_argument("Incompatible seed hashes: " + std::to_string(seed_hash) + ", "
            + std::to_string(compute_seed_hash(seed)));
      }
      FM85* uncompressed = fm85Uncompress(&compressed);
      delete [] compressed.compressedSurprisingValues;
      delete [] compressed.compressedWindow;
      cpc_sketch_unique_ptr sketch_ptr(
          new (fm85alloc(sizeof(cpc_sketch))) cpc_sketch(uncompressed, seed),
          [](cpc_sketch* s) { s->~cpc_sketch(); fm85free(s); }
      );
      return sketch_ptr;
    }

    static cpc_sketch_unique_ptr
    deserialize(const void* bytes, size_t size, uint64_t seed = DEFAULT_SEED) {
      fm85Init();
      const char* ptr = static_cast<const char*>(bytes);
      uint8_t preamble_ints;
      ptr += copy_from_mem(ptr, &preamble_ints, sizeof(preamble_ints));
      uint8_t serial_version;
      ptr += copy_from_mem(ptr, &serial_version, sizeof(serial_version));
      uint8_t family_id;
      ptr += copy_from_mem(ptr, &family_id, sizeof(family_id));
      uint8_t lg_k;
      ptr += copy_from_mem(ptr, &lg_k, sizeof(lg_k));
      uint8_t first_interesting_column;
      ptr += copy_from_mem(ptr, &first_interesting_column, sizeof(first_interesting_column));
      uint8_t flags_byte;
      ptr += copy_from_mem(ptr, &flags_byte, sizeof(flags_byte));
      uint16_t seed_hash;
      ptr += copy_from_mem(ptr, &seed_hash, sizeof(seed_hash));
      const bool has_hip(flags_byte & (1 << flags::HAS_HIP));
      const bool has_table(flags_byte & (1 << flags::HAS_TABLE));
      const bool has_window(flags_byte & (1 << flags::HAS_WINDOW));
      FM85 compressed;
      compressed.isCompressed = 1;
      compressed.mergeFlag = has_hip ? 0 : 1;
      compressed.lgK = lg_k;
      compressed.firstInterestingColumn = first_interesting_column;
      compressed.numCoupons = 0;
      compressed.numCompressedSurprisingValues = 0;
      compressed.kxp = 1 << lg_k;
      compressed.hipEstAccum = 0;
      compressed.hipErrAccum = 0;
      compressed.csvLength = 0;
      compressed.cwLength = 0;
      compressed.compressedSurprisingValues = nullptr;
      compressed.compressedWindow = nullptr;
      compressed.surprisingValueTable = nullptr;
      compressed.slidingWindow = nullptr;
      if (has_table || has_window) {
        uint32_t num_coupons;
        ptr += copy_from_mem(ptr, &num_coupons, sizeof(num_coupons));
        compressed.numCoupons = num_coupons;
        if (has_table && has_window) {
          uint32_t num_values;
          ptr += copy_from_mem(ptr, &num_values, sizeof(num_values));
          compressed.numCompressedSurprisingValues = num_values;
          if (has_hip) ptr += copy_hip_from_mem(&compressed, ptr);
        }
        if (has_table) {
          uint32_t csv_length;
          ptr += copy_from_mem(ptr, &csv_length, sizeof(csv_length));
          compressed.csvLength = csv_length;
        }
        if (has_window) {
          uint32_t cw_length;
          ptr += copy_from_mem(ptr, &cw_length, sizeof(cw_length));
          compressed.cwLength = cw_length;
        }
        if (has_hip && !(has_table && has_window)) ptr += copy_hip_from_mem(&compressed, ptr);
        if (has_window) {
          compressed.compressedWindow = new uint32_t[compressed.cwLength];
          ptr += copy_from_mem(ptr, compressed.compressedWindow, compressed.cwLength * sizeof(uint32_t));
        }
        if (has_table) {
          compressed.compressedSurprisingValues = new uint32_t[compressed.csvLength];
          ptr += copy_from_mem(ptr, compressed.compressedSurprisingValues, compressed.csvLength * sizeof(uint32_t));
        }
        if (!has_window) compressed.numCompressedSurprisingValues = compressed.numCoupons;
      }
      if (ptr != static_cast<const char*>(bytes) + size) throw std::logic_error("deserialized size mismatch");
      compressed.windowOffset = determineCorrectOffset(compressed.lgK, compressed.numCoupons);

      uint8_t expected_preamble_ints(get_preamble_ints(&compressed));
      if (preamble_ints != expected_preamble_ints) {
        throw std::invalid_argument("Possible corruption: preamble ints: expected "
            + std::to_string(expected_preamble_ints) + ", got " + std::to_string(preamble_ints));
      }
      if (serial_version != SERIAL_VERSION) {
        throw std::invalid_argument("Possible corruption: serial version: expected "
            + std::to_string(SERIAL_VERSION) + ", got " + std::to_string(serial_version));
      }
      if (family_id != FAMILY) {
        throw std::invalid_argument("Possible corruption: family: expected "
            + std::to_string(FAMILY) + ", got " + std::to_string(family_id));
      }
      if (seed_hash != compute_seed_hash(seed)) {
        throw std::invalid_argument("Incompatible seed hashes: " + std::to_string(seed_hash) + ", "
            + std::to_string(compute_seed_hash(seed)));
      }
      FM85* uncompressed = fm85Uncompress(&compressed);
      delete [] compressed.compressedSurprisingValues;
      delete [] compressed.compressedWindow;
      cpc_sketch_unique_ptr sketch_ptr(
          new (fm85alloc(sizeof(cpc_sketch))) cpc_sketch(uncompressed, seed),
          [](cpc_sketch* s) { s->~cpc_sketch(); fm85free(s); }
      );
      return sketch_ptr;
    }

    // for debugging
    uint64_t get_num_coupons() const {
      return state->numCoupons;
    }

    // for debugging
    // this should catch some forms of corruption during serialization-deserialization
    bool validate() const {
      U64* bit_matrix = bitMatrixOfSketch(state);
      const long long num_bits_set = countBitsSetInMatrix(bit_matrix, 1LL << state->lgK);
      fm85free(bit_matrix);
      return num_bits_set == state->numCoupons;
    }

    friend std::ostream& operator<<(std::ostream& os, cpc_sketch const& sketch);

    friend class cpc_union;

  private:
    static const uint8_t SERIAL_VERSION = 1;
    static const uint8_t FAMILY = 16;

    enum flags { IS_BIG_ENDIAN, IS_COMPRESSED, HAS_HIP, HAS_TABLE, HAS_WINDOW };

    FM85* state;
    uint64_t seed;

    // for deserialization and cpc_union::get_result()
    cpc_sketch(FM85* state, uint64_t seed = DEFAULT_SEED) : state(state), seed(seed) {}

    static uint8_t get_preamble_ints(const FM85* state) {
      uint8_t preamble_ints(2);
      if (state->numCoupons > 0) {
        preamble_ints += 1; // number of coupons
        if (!state->mergeFlag) {
          preamble_ints += 4; // HIP
        }
        if (state->compressedSurprisingValues != nullptr) {
          preamble_ints += 1; // table length
          // number of values (if there is no window it is the same as number of coupons)
          if (state->compressedWindow != nullptr) {
            preamble_ints += 1;
          }
        }
        if (state->compressedWindow != nullptr) {
          preamble_ints += 1; // window length
        }
      }
      return preamble_ints;
    }

    static inline void write_hip(const FM85* state, std::ostream& os) {
      os.write((char*)&state->kxp, sizeof(FM85::kxp));
      os.write((char*)&state->hipEstAccum, sizeof(FM85::hipEstAccum));
    }

    static inline void read_hip(FM85* state, std::istream& is) {
      is.read((char*)&state->kxp, sizeof(FM85::kxp));
      is.read((char*)&state->hipEstAccum, sizeof(FM85::hipEstAccum));
    }

    static inline size_t copy_hip_to_mem(const FM85* state, void* dst) {
      memcpy(dst, &state->kxp, sizeof(FM85::kxp));
      memcpy(static_cast<char*>(dst) + sizeof(FM85::kxp), &state->hipEstAccum, sizeof(FM85::hipEstAccum));
      return sizeof(FM85::kxp) + sizeof(FM85::hipEstAccum);
    }

    static inline size_t copy_hip_from_mem(FM85* state, const void* src) {
      memcpy(&state->kxp, src, sizeof(FM85::kxp));
      memcpy(&state->hipEstAccum, static_cast<const char*>(src) + sizeof(FM85::kxp), sizeof(FM85::hipEstAccum));
      return sizeof(FM85::kxp) + sizeof(FM85::hipEstAccum);
    }

    static inline size_t copy_to_mem(void* dst, const void* src, size_t size) {
      memcpy(dst, src, size);
      return size;
    }

    static inline size_t copy_from_mem(const void* src, void* dst, size_t size) {
      memcpy(dst, src, size);
      return size;
    }
};

} /* namespace datasketches */

#endif
