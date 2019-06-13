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

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include "fm85Compression.h"
#include "MurmurHash3.h"

namespace datasketches {

class compression_test: public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(compression_test);
  CPPUNIT_TEST(compress_and_uncompress_pairs);
  CPPUNIT_TEST_SUITE_END();

public:
  void setUp() {
    fm85Init();
  }

  void compress_and_uncompress_pairs() {
    const int N = 200;
    const int MAXWORDS = 1000;

    HashState twoHashes;
    U32 pairArray[N];
    U32 pairArray2[N];
    U64 value = 35538947; // some arbitrary starting value
    const U64 golden64 = 0x9e3779b97f4a7c13ULL;  // the golden ratio
    for (int i = 0; i < N; i++) {
      MurmurHash3_x64_128(&value, sizeof(value), 0, twoHashes);
      U32 rand = twoHashes.h1 & 0xffff;
      pairArray[i] = rand;
      value += golden64;
    }
    u32KnuthShellSort3(pairArray, 0L, (Long) (N-1));  // unsigned numerical sort
    U32 prev = ALL32BITS;
    int nxt = 0;
    for (int i = 0; i < N; i++) {     // uniquify
      if (pairArray[i] != prev) {
        prev = pairArray[i];
        pairArray[nxt++] = pairArray[i];
      }
    }
    int numPairs = nxt;
    //printf ("numPairs = %d\n", numPairs);
    //  for (int i = 0; i < numPairs; i++) {
    //    printf ("%d: %d %d\n", i, pairArray[i] >> 6, pairArray[i] & 63);
    //  }

    U32 compressedWords[MAXWORDS];

    for (Long numBaseBits = 0; numBaseBits <= 11; numBaseBits++) {
      Long numWordsWritten = lowLevelCompressPairs(pairArray, (Long) numPairs, numBaseBits, compressedWords);
      //printf ("numWordsWritten = %lld (numBaseBits = %lld)\n", numWordsWritten, numBaseBits);

      lowLevelUncompressPairs(pairArray2, (Long) numPairs, numBaseBits, compressedWords, numWordsWritten);
      for (int i = 0; i < numPairs; i++) {
        CPPUNIT_ASSERT(pairArray[i] == pairArray2[i]);
      }
    }
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(compression_test);

} /* namespace datasketches */
