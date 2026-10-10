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

#ifndef COMPACT_THETA_SKETCH_PARSER_HPP_
#define COMPACT_THETA_SKETCH_PARSER_HPP_

#include <cstdint>

namespace datasketches {

template<bool dummy>
class compact_theta_sketch_parser {
public:
  struct compact_theta_sketch_data {
    bool is_empty;
    bool is_ordered;
    uint16_t seed_hash;
    uint32_t num_entries;
    uint64_t theta;
    const void* entries_start_ptr;
    uint8_t entry_bits;
  };

  static compact_theta_sketch_data parse(const void* ptr, size_t size, uint64_t seed, bool dump_on_error = false);
  static void check_v4_entry_bits(uint8_t entry_bits);
  static void check_v4_num_entries_bytes(uint8_t num_entries_bytes);

  /**
   * Checks the first 8 bytes of a serial version 3 compact sketch that has the empty flag set.
   * Accepts the current 8-byte form (preamble longs 1), and the legacy 24-byte form
   * (preamble longs 3) written by Java before 1.0.0 for sketches with p < 1.
   * The legacy form must also carry the expected seed hash.
   * The caller must check that a legacy image is 24 bytes long and has no entries.
   * @param pre0 the first 8 bytes of the image, little-endian
   * @param expected_seed_hash the seed hash computed from the expected seed
   * @return the number of preamble longs: 1 or 3
   * @throw std::invalid_argument if the image is not a valid empty compact sketch
   */
  static uint8_t check_empty_v3(uint64_t pre0, uint16_t expected_seed_hash);

  // The MASK selects which bits of the first 8 bytes of an empty image are examined.
  // The TEST gives the required values of the examined bits; it must lie within the MASK.
  // Examined: preamble longs, serial version 3, sketch type 3, bytes 3 and 4 = 0 (not used by compact);
  //  flags read-only, empty, compact set; single-item and reserved bits 0, 6, 7 clear.
  // The ordered flag is ignored: C++ before 3.3.0 and Java before 1.0.0 wrote empty images without it.
  // The reserved flag bits must be zero for serial version 3. Any future use of them requires a new one.
  // The seed hash is ignored in the 8-byte form: it may be 0 or the seed hash, depending on the writer.
  // These must stay the same as in Java EmptyCompactSketch.
  static const uint64_t EMPTY_SKETCH_MASK = 0x0000EFFFFFFFFFFFULL;
  static const uint64_t EMPTY_SKETCH_TEST = 0x00000E0000030301ULL;
  static const uint64_t EMPTY_SKETCH_TEST_LEGACY = 0x00000E0000030303ULL;

private:
  // offsets are in sizeof(type)
  static const size_t COMPACT_SKETCH_PRE_LONGS_BYTE = 0;
  static const size_t COMPACT_SKETCH_SERIAL_VERSION_BYTE = 1;
  static const size_t COMPACT_SKETCH_TYPE_BYTE = 2;
  static const size_t COMPACT_SKETCH_FLAGS_BYTE = 5;
  static const size_t COMPACT_SKETCH_SEED_HASH_U16 = 3;
  static const size_t COMPACT_SKETCH_SINGLE_ENTRY_U64 = 1; // ver 3
  static const size_t COMPACT_SKETCH_NUM_ENTRIES_U32 = 2; // ver 1-3
  static const size_t COMPACT_SKETCH_ENTRIES_EXACT_U64 = 2; // ver 1-3
  static const size_t COMPACT_SKETCH_ENTRIES_ESTIMATION_U64 = 3; // ver 1-3
  static const size_t COMPACT_SKETCH_THETA_U64 = 2; // ver 1-3
  static const size_t COMPACT_SKETCH_V4_ENTRY_BITS_BYTE = 3;
  static const size_t COMPACT_SKETCH_V4_NUM_ENTRIES_BYTES_BYTE = 4;
  static const size_t COMPACT_SKETCH_V4_THETA_U64 = 1;
  static const size_t COMPACT_SKETCH_V4_PACKED_DATA_EXACT_BYTE = 8;
  static const size_t COMPACT_SKETCH_V4_PACKED_DATA_ESTIMATION_BYTE = 16;

  static const uint8_t COMPACT_SKETCH_IS_EMPTY_FLAG = 2;
  static const uint8_t COMPACT_SKETCH_IS_ORDERED_FLAG = 4;

  static const uint8_t COMPACT_SKETCH_TYPE = 3;

  static void check_memory_size(const void* ptr, size_t actual_bytes, size_t expected_bytes, bool dump_on_error);
  static std::string hex_dump(const uint8_t* ptr, size_t size);
};

} /* namespace datasketches */

#include "compact_theta_sketch_parser_impl.hpp"

#endif
