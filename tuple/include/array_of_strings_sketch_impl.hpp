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

/**
 * Feeds strings to XXHash64 as UTF-16LE code units, which is how Java hashes
 * a String with XxHash.hashCharArr(s.toCharArray(), ...).
 *
 * The input is decoded from UTF-8. Malformed input is replaced with U+FFFD exactly
 * as Java's UTF-8 decoder (new String(bytes, StandardCharsets.UTF_8)) does it:
 * one U+FFFD for the longest valid prefix of a malformed sequence (at least one byte),
 * and one U+FFFD for a complete 3-byte sequence that encodes a surrogate.
 */
class array_of_strings_key_hasher {
public:
  explicit array_of_strings_key_hasher(uint64_t seed): hasher_(seed), size_(0) {}

  void add_utf8(const std::string& str) {
    const uint8_t* bytes = reinterpret_cast<const uint8_t*>(str.data());
    const size_t length = str.size();
    size_t i = 0;
    while (i < length) {
      const uint8_t lead = bytes[i++];
      if (lead < 0x80) { // ASCII
        add_code_unit(lead);
        continue;
      }
      uint8_t second_min;
      uint8_t second_max;
      const unsigned num_continuation = get_sequence_rule(lead, second_min, second_max);
      if (num_continuation == 0) { // Invalid UTF8
        add_code_unit(REPLACEMENT_CHAR);
        continue;
      }
      uint32_t code_point = lead & (0x3F >> num_continuation);
      unsigned num_read = 0;
      while (num_read < num_continuation && i < length) {
        const uint8_t byte = bytes[i];
        const bool valid = num_read == 0 ? byte >= second_min && byte <= second_max : (byte & 0xC0) == 0x80;
        if (!valid) break;
        code_point = (code_point << 6) | (byte & 0x3F);
        ++i;
        ++num_read;
      }
      if (num_read < num_continuation) {
        // the bytes consumed so far are the longest valid prefix, resume at the next byte
        add_code_unit(REPLACEMENT_CHAR);
      } else {
        add_code_point(code_point);
      }
    }
  }

  void add_code_unit(uint16_t unit) {
    if (size_ == sizeof(buffer_)) flush();
    buffer_[size_++] = static_cast<uint8_t>(unit);
    buffer_[size_++] = static_cast<uint8_t>(unit >> 8);
  }

  uint64_t hash() {
    flush();
    return hasher_.hash();
  }

private:
  enum : uint16_t { REPLACEMENT_CHAR = 0xFFFD };

  XXHash64 hasher_;
  uint8_t buffer_[256];
  size_t size_;

  void flush() {
    hasher_.add(buffer_, size_);
    size_ = 0;
  }

  void add_code_point(uint32_t code_point) {
    if (code_point >= 0xD800 && code_point <= 0xDFFF) {
      add_code_unit(REPLACEMENT_CHAR);
    } else if (code_point < 0x10000) {
      add_code_unit(static_cast<uint16_t>(code_point));
    } else {
      code_point -= 0x10000;
      add_code_unit(static_cast<uint16_t>(0xD800 | (code_point >> 10)));
      add_code_unit(static_cast<uint16_t>(0xDC00 | (code_point & 0x3FF)));
    }
  }

  /**
   * Returns the number of continuation bytes that follow a UTF-8 lead byte,
   * or 0 if the byte cannot start a sequence (80-C1, F5-FF).
   * Also returns the range the second byte must be in, as Java's decoder checks it.
   */
  static unsigned get_sequence_rule(uint8_t lead, uint8_t& second_min, uint8_t& second_max) {
    second_min = 0x80;
    second_max = 0xBF;
    if (lead >= 0xC2 && lead <= 0xDF) return 1;
    if (lead == 0xE0) second_min = 0xA0; // no overlong forms
    // ED A0-BF encodes a surrogate, which Java's decoder accepts here and replaces once decoded
    if (lead >= 0xE0 && lead <= 0xEF) return 2;
    if (lead == 0xF0) second_min = 0x90; // no overlong forms
    if (lead == 0xF4) second_max = 0x8F; // nothing above U+10FFFF
    if (lead >= 0xF0 && lead <= 0xF4) return 3;
    return 0;
  }
};

inline uint64_t hash_array_of_strings_key(const array_of_strings& key) {
  // Matches Java Util.PRIME for ArrayOfStrings key hashing.
  static constexpr uint64_t STRING_ARR_HASH_SEED = 0x7A3CCA71ULL;
  array_of_strings_key_hasher hasher(STRING_ARR_HASH_SEED);
  const auto size = static_cast<size_t>(key.size());
  for (size_t i = 0; i < size; ++i) {
    if (i > 0) hasher.add_code_unit(',');
    hasher.add_utf8(key[i]);
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
void compact_array_of_strings_tuple_sketch<Allocator>::serialize(std::ostream& os, const SerDe& sd) const {
  Base::serialize(os, sd);
}

template<typename Allocator>
template<typename SerDe>
auto compact_array_of_strings_tuple_sketch<Allocator>::serialize(
  unsigned header_size_bytes, const SerDe& sd
) const -> vector_bytes {
  return Base::serialize(header_size_bytes, sd);
}

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
      const uint8_t num_nodes = items[i].size();
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
