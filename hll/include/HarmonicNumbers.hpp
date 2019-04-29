/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _HARMONICNUMBERS_HPP_
#define _HARMONICNUMBERS_HPP_

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
    static double getBitMapEstimate(int bitVectorLength, int numBitsSet);

  private:
    static double harmonicNumber(uint64_t x_i);
};

}

//#include "HarmonicNumbers-internal.hpp"

#endif /* _HARMONICNUMBERS_HPP_ */