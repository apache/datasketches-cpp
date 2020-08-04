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

namespace datasketches {

template<int num, typename A>
compact_array_of_doubles_sketch<num, A>::compact_array_of_doubles_sketch(const Base& other, bool ordered):
Base(other, ordered) {}

template<int num, typename A>
compact_array_of_doubles_sketch<num, A>::compact_array_of_doubles_sketch(bool is_empty, bool is_ordered, uint16_t seed_hash, uint64_t theta, std::vector<Entry, AllocEntry>&& entries):
Base(is_empty, is_ordered, seed_hash, theta, std::move(entries)) {}

template<int num, typename A>
compact_array_of_doubles_sketch<num, A> compact_array_of_doubles_sketch<num, A>::deserialize(std::istream& is, uint64_t seed, const A& allocator) {
  uint8_t preamble_longs;
  is.read((char*)&preamble_longs, sizeof(preamble_longs));
  uint8_t serial_version;
  is.read((char*)&serial_version, sizeof(serial_version));
  uint8_t family;
  is.read((char*)&family, sizeof(family));
  uint8_t type;
  is.read((char*)&type, sizeof(type));
  uint8_t flags_byte;
  is.read((char*)&flags_byte, sizeof(flags_byte));
  uint8_t num_values;
  is.read((char*)&num_values, sizeof(num_values));
  uint16_t seed_hash;
  is.read((char*)&seed_hash, sizeof(seed_hash));
  checker<true>::check_sketch_family(family, SKETCH_FAMILY);
  checker<true>::check_sketch_type(type, SKETCH_TYPE);
  checker<true>::check_serial_version(serial_version, SERIAL_VERSION);
  const bool is_empty = flags_byte & (1 << flags::IS_EMPTY);
  if (!is_empty) checker<true>::check_seed_hash(seed_hash, compute_seed_hash(seed));

  uint64_t theta;
  is.read((char*)&theta, sizeof(theta));
  uint32_t num_entries = 0;
  if (!is_empty) {
    is.read((char*)&num_entries, sizeof(num_entries));
    uint32_t unused32;
    is.read((char*)&unused32, sizeof(unused32));
  }
  std::vector<Entry, AllocEntry> entries(allocator);
  if (!is_empty) {
    entries.reserve(num_entries);
    std::vector<uint64_t, AllocU64> keys(num_entries, 0, allocator);
    is.read((char*)keys.data(), num_entries * sizeof(uint64_t));
    std::vector<Summary, A> summaries(num_entries, Summary(), allocator);
    is.read((char*)summaries.data(), num_entries * sizeof(Summary));
    for (size_t i = 0; i < num_entries; ++i) {
      entries.push_back(Entry(keys[i], summaries[i]));
    }
  }
  if (!is.good()) throw std::runtime_error("error reading from std::istream");
  const bool is_ordered = flags_byte & (1 << flags::IS_ORDERED);
  return compact_array_of_doubles_sketch(is_empty, is_ordered, seed_hash, theta, std::move(entries));
}

} /* namespace datasketches */
