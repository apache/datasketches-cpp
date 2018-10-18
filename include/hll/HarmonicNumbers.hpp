/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#pragma once

#include <cstdint>

namespace datasketches {

class HarmonicNumbers {
  public:
    /**
     * This is the estimator you would use for flat bit map random accessed, similar to a Bloom filter.
     * @param bitVectorLength the length of the bit vector in bits. Must be &gt; 0.
     * @param numBitsSet the number of bits set in this bit vector. Must be &ge; 0 and &le;
     * bitVectorLength.
     * @return the estimate.
     */
    static double getBitMapEstimate(const int bitVectorLength, const int numBitsSet);

  private:
    static double harmonicNumber(const uint64_t x_i);
};

}
