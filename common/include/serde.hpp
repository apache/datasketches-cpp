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

#ifndef DATASKETCHES_SERDE_HPP_
#define DATASKETCHES_SERDE_HPP_

#include <memory>
#include <cstring>

namespace datasketches {

// serialize and deserialize
template<typename T> struct serde {
  // stream
  void serialize(std::ostream& os, const T* items, unsigned num);
  void deserialize(std::istream& is, T* items, unsigned num); // items are not initialized
  // raw bytes
  size_t size_of_item(const T& item);
  size_t serialize(char* ptr, const T* items, unsigned num);
  size_t deserialize(const char* ptr, T* items, unsigned num); // items are not initialized
};

template<>
struct serde<int32_t> {
  void serialize(std::ostream& os, const int32_t* items, unsigned num) {
    os.write((char*)items, sizeof(int32_t) * num);
  }
  void deserialize(std::istream& is, int32_t* items, unsigned num) {
    is.read((char*)items, sizeof(int32_t) * num);
  }
  size_t size_of_item(int32_t item) {
    return sizeof(int32_t);
  }
  size_t serialize(char* ptr, const int32_t* items, unsigned num) {
    memcpy(ptr, items, sizeof(int32_t) * num);
    return sizeof(int32_t) * num;
  }
  size_t deserialize(const char* ptr, int32_t* items, unsigned num) {
    memcpy(items, ptr, sizeof(int32_t) * num);
    return sizeof(int32_t) * num;
  }
};

// serde for signed 64-bit integers
// this should produce sketches binary-compatible with LongsSketch
// and ItemsSketch<Long> with ArrayOfLongsSerDe in Java
template<>
struct serde<int64_t> {
  void serialize(std::ostream& os, const int64_t* items, unsigned num) {
    os.write((char*)items, sizeof(int64_t) * num);
  }
  void deserialize(std::istream& is, int64_t* items, unsigned num) {
    is.read((char*)items, sizeof(int64_t) * num);
  }
  size_t size_of_item(int64_t item) {
    return sizeof(int64_t);
  }
  size_t serialize(char* ptr, const int64_t* items, unsigned num) {
    memcpy(ptr, items, sizeof(int64_t) * num);
    return sizeof(int64_t) * num;
  }
  size_t deserialize(const char* ptr, int64_t* items, unsigned num) {
    memcpy(items, ptr, sizeof(int64_t) * num);
    return sizeof(int64_t) * num;
  }
};

template<>
struct serde<float> {
  void serialize(std::ostream& os, const float* items, unsigned num) {
    os.write((char*)items, sizeof(float) * num);
  }
  void deserialize(std::istream& is, float* items, unsigned num) {
    is.read((char*)items, sizeof(float) * num);
  }
  size_t size_of_item(float item) {
    return sizeof(float);
  }
  size_t serialize(char* ptr, const float* items, unsigned num) {
    memcpy(ptr, items, sizeof(float) * num);
    return sizeof(float) * num;
  }
  size_t deserialize(const char* ptr, float* items, unsigned num) {
    memcpy(items, ptr, sizeof(float) * num);
    return sizeof(float) * num;
  }
};

// serde for std::string items
// This should produce sketches binary-compatible with
// ItemsSketch<String> with ArrayOfStringsSerDe in Java.
// The length of each string is stored as a 32-bit integer (historically),
// which may be too wasteful. Treat this as an example.
template<>
struct serde<std::string> {
  void serialize(std::ostream& os, const std::string* items, unsigned num) {
    for (unsigned i = 0; i < num; i++) {
      uint32_t length = items[i].size();
      os.write((char*)&length, sizeof(length));
      os.write(items[i].c_str(), length);
    }
  }
  void deserialize(std::istream& is, std::string* items, unsigned num) {
    for (unsigned i = 0; i < num; i++) {
      uint32_t length;
      is.read((char*)&length, sizeof(length));
      new (&items[i]) std::string;
      items[i].reserve(length);
      auto it = std::istreambuf_iterator<char>(is);
      for (uint32_t j = 0; j < length; j++) {
        items[i].push_back(*it);
        ++it;
      }
    }
  }
  size_t size_of_item(const std::string& item) {
    return sizeof(uint32_t) + item.size();
  }
  size_t serialize(char* ptr, const std::string* items, unsigned num) {
    size_t size = sizeof(uint32_t) * num;
    for (unsigned i = 0; i < num; i++) {
      uint32_t length = items[i].size();
      memcpy(ptr, &length, sizeof(length));
      ptr += sizeof(uint32_t);
      memcpy(ptr, items[i].c_str(), length);
      ptr += length;
      size += length;
    }
    return size;
  }
  size_t deserialize(const char* ptr, std::string* items, unsigned num) {
    size_t size = sizeof(uint32_t) * num;
    for (unsigned i = 0; i < num; i++) {
      uint32_t length;
      memcpy(&length, ptr, sizeof(length));
      ptr += sizeof(uint32_t);
      new (&items[i]) std::string(ptr, length);
      ptr += length;
      size += length;
    }
    return size;
  }
};

static inline void copy_from_mem(const char** src, void* dst, size_t size) {
  memcpy(dst, *src, size);
  *src += size;
}

static inline void copy_to_mem(const void* src, char** dst, size_t size) {
  memcpy(*dst, src, size);
  *dst += size;
}

} /* namespace datasketches */

# endif
