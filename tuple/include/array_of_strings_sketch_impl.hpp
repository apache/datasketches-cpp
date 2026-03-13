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

#ifndef ARRAY_OF_STRINGS_SKETCH_IMPL_HPP_
#define ARRAY_OF_STRINGS_SKETCH_IMPL_HPP_

#include <stdexcept>

#include "common_defs.hpp"

namespace datasketches {

inline array_of_strings default_array_of_strings_update_policy::create() const {
  return array_of_strings(0, "");
}

inline void default_array_of_strings_update_policy::update(
  array_of_strings& array, const array_of_strings& input
) const {
  const auto length = static_cast<size_t>(input.size());
  array = array_of_strings(static_cast<uint8_t>(length), "");
  for (size_t i = 0; i < length; ++i) array[i] = input[i];
}

inline void default_array_of_strings_update_policy::update(
  array_of_strings& array, const array_of_strings* input
) const {
  if (input == nullptr) {
    array = array_of_strings(0, "");
    return;
  }
  const auto length = static_cast<size_t>(input->size());
  array = array_of_strings(static_cast<uint8_t>(length), "");
  for (size_t i = 0; i < length; ++i) array[i] = (*input)[i];
}

inline uint64_t hash_array_of_strings_key(const array_of_strings& key) {
  // Matches Java Util.PRIME for ArrayOfStrings key hashing.
  static constexpr uint64_t STRING_ARR_HASH_SEED = 0x7A3CCA71ULL;
  XXHash64 hasher(STRING_ARR_HASH_SEED);
  const auto size = static_cast<size_t>(key.size());
  for (size_t i = 0; i < size; ++i) {
    const auto& entry = key[i];
    hasher.add(entry.data(), entry.size());
    if (i + 1 < size) hasher.add(",", 1);
  }
  return hasher.hash();
}

template<typename Allocator, typename Policy>
compact_array_of_strings_tuple_sketch<Allocator> compact_array_of_strings_sketch(
  const update_array_of_strings_tuple_sketch<Allocator, Policy>& sketch, bool ordered
) {
  return compact_array_of_strings_tuple_sketch<Allocator>(sketch, ordered);
}

template<typename Allocator>
template<typename Sketch>
compact_array_of_strings_tuple_sketch<Allocator>::compact_array_of_strings_tuple_sketch(
  const Sketch& sketch, bool ordered
): Base(sketch, ordered) {}

template<typename Allocator>
compact_array_of_strings_tuple_sketch<Allocator>::compact_array_of_strings_tuple_sketch(
  Base&& base
): Base(std::move(base)) {}

template<typename Allocator>
template<typename SerDe>
auto compact_array_of_strings_tuple_sketch<Allocator>::deserialize(
  std::istream& is, uint64_t seed, const SerDe& sd, const Allocator& allocator
) -> compact_array_of_strings_tuple_sketch {
  auto base = Base::deserialize(is, seed, sd, allocator);
  return compact_array_of_strings_tuple_sketch(std::move(base));
}

template<typename Allocator>
template<typename SerDe>
auto compact_array_of_strings_tuple_sketch<Allocator>::deserialize(
  const void* bytes, size_t size, uint64_t seed, const SerDe& sd, const Allocator& allocator
) -> compact_array_of_strings_tuple_sketch {
  auto base = Base::deserialize(bytes, size, seed, sd, allocator);
  return compact_array_of_strings_tuple_sketch(std::move(base));
}

template<typename Allocator>
default_array_of_strings_serde<Allocator>::default_array_of_strings_serde(const Allocator& allocator):
  summary_allocator_(allocator) {}

template<typename Allocator>
void default_array_of_strings_serde<Allocator>::serialize(
  std::ostream& os, const array_of_strings* items, unsigned num
) const {
  unsigned i = 0;
  try {
    for (; i < num; ++i) {
      const uint32_t total_bytes = compute_total_bytes(items[i]);
      const uint8_t num_nodes = static_cast<uint8_t>(items[i].size());
      write(os, total_bytes);
      write(os, num_nodes);
      const std::string* data = items[i].data();
      for (uint8_t j = 0; j < num_nodes; ++j) {
        const uint32_t length = static_cast<uint32_t>(data[j].size());
        write(os, length);
        os.write(data[j].data(), length);
      }
    }
  } catch (std::runtime_error& e) {
    if (std::string(e.what()).find("size exceeds 127") != std::string::npos) throw;
    throw std::runtime_error("array_of_strings stream write failed at item " + std::to_string(i));
  }
}

template<typename Allocator>
void default_array_of_strings_serde<Allocator>::deserialize(
  std::istream& is, array_of_strings* items, unsigned num
) const {
  unsigned i = 0;
  bool failure = false;
  try {
    for (; i < num; ++i) {
      read<uint32_t>(is); // total_bytes
      if (!is) { failure = true; break; }
      const uint8_t num_nodes = read<uint8_t>(is);
      if (!is) { failure = true; break; }
      check_num_nodes(num_nodes);
      array_of_strings array(num_nodes, "");
      for (uint8_t j = 0; j < num_nodes; ++j) {
        const uint32_t length = read<uint32_t>(is);
        if (!is) { failure = true; break; }
        std::string value(length, '\0');
        if (length != 0) {
          is.read(&value[0], length);
          if (!is) { failure = true; break; }
        }
        array[j] = std::move(value);
      }
      if (failure) break;
      summary_allocator alloc(summary_allocator_);
      std::allocator_traits<summary_allocator>::construct(alloc, &items[i], std::move(array));
    }
  } catch (std::exception& e) {
    summary_allocator alloc(summary_allocator_);
    for (unsigned j = 0; j < i; ++j) {
      std::allocator_traits<summary_allocator>::destroy(alloc, &items[j]);
    }
    if (std::string(e.what()).find("size exceeds 127") != std::string::npos) throw;
    throw std::runtime_error("array_of_strings stream read failed at item " + std::to_string(i));
  }
  if (failure) {
    summary_allocator alloc(summary_allocator_);
    for (unsigned j = 0; j < i; ++j) {
      std::allocator_traits<summary_allocator>::destroy(alloc, &items[j]);
    }
    throw std::runtime_error("array_of_strings stream read failed at item " + std::to_string(i));
  }
}

template<typename Allocator>
size_t default_array_of_strings_serde<Allocator>::serialize(
  void* ptr, size_t capacity, const array_of_strings* items, unsigned num
) const {
  uint8_t* ptr8 = static_cast<uint8_t*>(ptr);
  size_t bytes_written = 0;

  for (unsigned i = 0; i < num; ++i) {
    const uint32_t total_bytes = compute_total_bytes(items[i]);
    const uint8_t num_nodes = static_cast<uint8_t>(items[i].size());
    check_memory_size(bytes_written + total_bytes, capacity);
    bytes_written += copy_to_mem(total_bytes, ptr8 + bytes_written);
    bytes_written += copy_to_mem(num_nodes, ptr8 + bytes_written);
    const std::string* data = items[i].data();
    for (uint8_t j = 0; j < num_nodes; ++j) {
      const uint32_t length = static_cast<uint32_t>(data[j].size());

      bytes_written += copy_to_mem(length, ptr8 + bytes_written);
      bytes_written += copy_to_mem(data[j].data(), ptr8 + bytes_written, length);
    }
  }
  return bytes_written;
}

template<typename Allocator>
size_t default_array_of_strings_serde<Allocator>::deserialize(
  const void* ptr, size_t capacity, array_of_strings* items, unsigned num
) const {
  const uint8_t* ptr8 = static_cast<const uint8_t*>(ptr);
  size_t bytes_read = 0;

  unsigned i = 0;

  try {
    for (; i < num; ++i) {
      check_memory_size(bytes_read + sizeof(uint32_t), capacity);
      const size_t item_start = bytes_read;
      uint32_t total_bytes;
      bytes_read += copy_from_mem(ptr8 + bytes_read, total_bytes);
      check_memory_size(item_start + total_bytes, capacity);

      check_memory_size(bytes_read + sizeof(uint8_t), capacity);
      uint8_t num_nodes;
      bytes_read += copy_from_mem(ptr8 + bytes_read, num_nodes);
      check_num_nodes(num_nodes);

      array_of_strings array(num_nodes, "");
      for (uint8_t j = 0; j < num_nodes; ++j) {
        check_memory_size(bytes_read + sizeof(uint32_t), capacity);
        uint32_t length;
        bytes_read += copy_from_mem(ptr8 + bytes_read, length);

        check_memory_size(bytes_read + length, capacity);
        std::string value(length, '\0');
        if (length != 0) {
          bytes_read += copy_from_mem(ptr8 + bytes_read, &value[0], length);
        }
        array[j] = std::move(value);
      }
      summary_allocator alloc(summary_allocator_);
      std::allocator_traits<summary_allocator>::construct(alloc, &items[i], std::move(array));
    }
  } catch (std::exception& e) {
    summary_allocator alloc(summary_allocator_);
    for (unsigned j = 0; j < i; ++j) {
      std::allocator_traits<summary_allocator>::destroy(alloc, &items[j]);
    }
    if (std::string(e.what()).find("size exceeds 127") != std::string::npos) throw;
    throw std::runtime_error("array_of_strings bytes read failed at item " + std::to_string(i));
  }
  return bytes_read;
}

template<typename Allocator>
size_t default_array_of_strings_serde<Allocator>::size_of_item(const array_of_strings& item) const {
  return compute_total_bytes(item);
}

template<typename Allocator>
void default_array_of_strings_serde<Allocator>::check_num_nodes(uint8_t num_nodes) {
  if (num_nodes > 127) {
    throw std::runtime_error("array_of_strings size exceeds 127");
  }
}

template<typename Allocator>
uint32_t default_array_of_strings_serde<Allocator>::compute_total_bytes(const array_of_strings& item) {
  const auto count = item.size();
  check_num_nodes(static_cast<uint8_t>(count));
  size_t total = sizeof(uint32_t) + sizeof(uint8_t) + count * sizeof(uint32_t);
  const std::string* data = item.data();
  for (uint32_t j = 0; j < count; ++j) {
    total += data[j].size();
  }
  return static_cast<uint32_t>(total);
}

} /* namespace datasketches */

#endif
