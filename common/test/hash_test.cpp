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
#include <vector>

#include "MurmurHash3.h"
#include "xxhash64.h"

namespace datasketches {

// The expected values below were computed with the DataSketches Java MurmurHash3 and XxHash64,
// on the bytes (i * 7 + 1) for i = 0 .. length - 1, with seed 9001.
// The lengths cover every tail length of both hashes, and inputs below and above their block sizes
// (16 bytes for MurmurHash3, 32 bytes for XXH64).

static std::vector<uint8_t> test_bytes(size_t length) {
  std::vector<uint8_t> bytes(length);
  for (size_t i = 0; i < length; ++i) bytes[i] = static_cast<uint8_t>(i * 7 + 1);
  return bytes;
}

struct murmur_vector { size_t length; uint64_t h1; uint64_t h2; };

static const murmur_vector murmur_vectors[] = {
  {1, 0x07D0803C66013EC8ULL, 0x927863C2EEFD6912ULL},
  {7, 0x020B51C078D587CBULL, 0x316B3141E9FFD773ULL},
  {8, 0xE9F71E57E963903AULL, 0xA957E0843C44F3AAULL},
  {9, 0x6B6CB32940E705E3ULL, 0x3E7CD4000704ACDAULL},
  {15, 0x736A252A1C5E172DULL, 0x517E714AC2248BA4ULL},
  {16, 0xD8B586BF256F1DDCULL, 0xB5C448377D77B514ULL},
  {17, 0xC0A352AC625BAFFAULL, 0x0913B04F53B223CCULL},
  {31, 0x4B9F83F8F651658AULL, 0x1205B2CA952D9C19ULL},
  {32, 0x5B55CBEEB43D7D0BULL, 0x4DB30635C11EED0CULL},
  {33, 0x3867BF4C60D1A19AULL, 0xFF095481A58FDC70ULL},
  {47, 0x3EE5809A1A0C112AULL, 0x7DACD3C87CC9F091ULL},
  {48, 0x9E66BF0565E4F423ULL, 0x395DD92B981FEE6DULL},
  {63, 0x0FC165544C602A43ULL, 0x5BFB95F694356CC3ULL},
};

TEST_CASE("MurmurHash3_x64_128 matches Java", "[common]") {
  for (const auto& v: murmur_vectors) {
    CAPTURE(v.length);
    const auto bytes = test_bytes(v.length);
    HashState hashes;
    MurmurHash3_x64_128(bytes.data(), bytes.size(), 9001, hashes);
    REQUIRE(hashes.h1 == v.h1);
    REQUIRE(hashes.h2 == v.h2);
  }
}

struct xxhash64_vector { size_t length; uint64_t hash; };

static const xxhash64_vector xxhash64_vectors[] = {
  {0, 0x9C981E42BD7E64E7ULL},
  {1, 0xA0AB08F4B1C01485ULL},
  {3, 0xE65244A36F7F595DULL},
  {4, 0xF12CB5FF2035CE10ULL},
  {7, 0xBCEB60AC33C41D23ULL},
  {8, 0x84F082F72149F5FFULL},
  {31, 0x42B387B64FCB9962ULL},
  {32, 0x6FCB36F5CEDF1CD4ULL},
  {33, 0x98B48780014AF78CULL},
  {63, 0x0C72E0BAFADA60F5ULL},
  {64, 0xD80BE3B3B7780235ULL},
  {65, 0xCD0333A1820D1C01ULL},
  {100, 0xF2F5A9088E94AFF8ULL},
};

TEST_CASE("XXHash64 matches Java", "[common]") {
  for (const auto& v: xxhash64_vectors) {
    CAPTURE(v.length);
    const auto bytes = test_bytes(v.length);
    REQUIRE(XXHash64::hash(bytes.data(), bytes.size(), 9001) == v.hash);
  }
}

// test vectors from the xxHash specification, as listed in the Count-Min format specification in datasketches-tck
TEST_CASE("XXHash64 specification vectors", "[common]") {
  REQUIRE(XXHash64::hash("", 0, 0) == 0xEF46DB3751D8E999ULL);
  REQUIRE(XXHash64::hash("a", 1, 0) == 0xD24EC4F1A98C6E5BULL);
  REQUIRE(XXHash64::hash("abc", 3, 0) == 0x44BC2CF5AD770999ULL);
}

TEST_CASE("XXHash64 streaming gives the same hash as one call", "[common]") {
  const auto bytes = test_bytes(100);
  const uint64_t expected = XXHash64::hash(bytes.data(), bytes.size(), 9001);
  // split points before, inside and across the 32-byte blocks
  for (size_t first: {1, 5, 31, 32, 33, 50, 64, 99}) {
    CAPTURE(first);
    XXHash64 hasher(9001);
    REQUIRE(hasher.add(bytes.data(), first));
    REQUIRE(hasher.add(bytes.data() + first, bytes.size() - first));
    REQUIRE(hasher.hash() == expected);
  }
  // many small pieces
  XXHash64 hasher(9001);
  for (size_t i = 0; i < bytes.size(); i += 3) {
    REQUIRE(hasher.add(bytes.data() + i, std::min<size_t>(3, bytes.size() - i)));
  }
  REQUIRE(hasher.hash() == expected);
  // no data is rejected and changes nothing
  REQUIRE_FALSE(hasher.add(nullptr, 5));
  REQUIRE_FALSE(hasher.add(bytes.data(), 0));
  REQUIRE(hasher.hash() == expected);
}

} /* namespace datasketches */
