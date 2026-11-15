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

#include <catch2/catch.hpp>
#include <cstdint>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <streambuf>
#include <string>

#include "serde.hpp"

namespace datasketches {

// a stream buffer that accepts no output
struct failing_buffer: std::streambuf {
protected:
  int_type overflow(int_type) override { return traits_type::eof(); }
  std::streamsize xsputn(const char*, std::streamsize) override { return 0; }
};

// serde reports a stream error as std::runtime_error, whether or not the stream throws exceptions
TEST_CASE("serde: arithmetic write failure", "[common]") {
  const int64_t items[] = {1, 2, 3};
  for (bool exceptions: {false, true}) {
    CAPTURE(exceptions);
    failing_buffer buffer;
    std::ostream os(&buffer);
    if (exceptions) os.exceptions(std::ios::failbit | std::ios::badbit);
    REQUIRE_THROWS_AS(serde<int64_t>().serialize(os, items, 3), std::runtime_error);
  }
}

TEST_CASE("serde: arithmetic read failure", "[common]") {
  for (bool exceptions: {false, true}) {
    CAPTURE(exceptions);
    std::istringstream is(std::string(10, 'x')); // 10 bytes, 24 needed
    if (exceptions) is.exceptions(std::ios::failbit | std::ios::badbit);
    int64_t items[3];
    REQUIRE_THROWS_AS(serde<int64_t>().deserialize(is, items, 3), std::runtime_error);
  }
}

TEST_CASE("serde: string write failure", "[common]") {
  const std::string items[] = {"apple", "pear"};
  for (bool exceptions: {false, true}) {
    CAPTURE(exceptions);
    failing_buffer buffer;
    std::ostream os(&buffer);
    if (exceptions) os.exceptions(std::ios::failbit | std::ios::badbit);
    REQUIRE_THROWS_AS(serde<std::string>().serialize(os, items, 2), std::runtime_error);
  }
}

TEST_CASE("serde: string read failure", "[common]") {
  // the first string is complete, the second is cut short
  const std::string items[] = {"apple", "a longer string than the short string optimization holds"};
  std::stringstream full(std::ios::in | std::ios::out | std::ios::binary);
  serde<std::string>().serialize(full, items, 2);
  const std::string bytes = full.str();
  for (size_t cut: {bytes.size() - 1, size_t(4 + 5 + 2), size_t(4 + 2)}) {
    for (bool exceptions: {false, true}) {
      CAPTURE(cut, exceptions);
      std::istringstream is(bytes.substr(0, cut));
      if (exceptions) is.exceptions(std::ios::failbit | std::ios::badbit);
      // items are constructed by deserialize and destroyed by it on failure
      std::allocator<std::string> alloc;
      std::string* read = alloc.allocate(2);
      REQUIRE_THROWS_AS(serde<std::string>().deserialize(is, read, 2), std::runtime_error);
      alloc.deallocate(read, 2);
    }
  }
}

} /* namespace datasketches */
