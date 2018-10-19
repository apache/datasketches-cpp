/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#pragma once

#include "MurmurHash3.h"
#include "RelativeErrorTables.hpp"

#include <cassert>
#include <cmath>
#include <exception>
#include <string>
#include <sstream>
#include <stdexcept>

namespace datasketches {

enum CurMode { LIST = 0, SET, HLL };

class HllUtil {
public:
  // preamble stuff
  static const int SER_VER = 1;
  static const int FAMILY_ID = 7;

  static const int EMPTY_FLAG_MASK          = 4;
  static const int COMPACT_FLAG_MASK        = 8;
  static const int OUT_OF_ORDER_FLAG_MASK   = 16;

  // Coupon List
  static const int LIST_INT_ARR_START = 8;
  static const int LIST_PREINTS = 2;
  // Coupon Hash Set
  static const int HASH_SET_COUNT_INT             = 8;
  static const int HASH_SET_INT_ARR_START         = 12;
  static const int HASH_SET_PREINTS         = 3;
  // HLL
  static const int HLL_PREINTS = 10;
  static const int HLL_BYTE_ARR_START = 40;

  // other HllUtil stuff
  static const int KEY_BITS_26 = 26;
  static const int VAL_BITS_6 = 6;
  static const int KEY_MASK_26 = (1 << KEY_BITS_26) - 1;
  static const int VAL_MASK_6 = (1 << VAL_BITS_6) - 1;
  static const int EMPTY = 0;
  static const int MIN_LOG_K = 4;
  static const int MAX_LOG_K = 21;

  static const uint64_t DEFAULT_UPDATE_SEED = 9001L;

  static const double HLL_HIP_RSE_FACTOR; // sqrt(log(2.0)) = 0.8325546
  static const double HLL_NON_HIP_RSE_FACTOR; // sqrt((3.0 * log(2.0)) - 1.0) = 1.03896
  static const double COUPON_RSE_FACTOR; // 0.409 at transition point not the asymptote
  static const double COUPON_RSE; // COUPON_RSE_FACTOR / (1 << 13);

  static const int LG_INIT_LIST_SIZE = 3;
  static const int LG_INIT_SET_SIZE = 5;
  static const int RESIZE_NUMER = 3;
  static const int RESIZE_DENOM = 4;

  static const int loNibbleMask = 0x0f;
  static const int hiNibbleMask = 0xf0;
  static const int AUX_TOKEN = 0xf;

  /**
  * Log2 table sizes for exceptions based on lgK from 0 to 26.
  * However, only lgK from 4 to 21 are used.
  */
  static const int LG_AUX_ARR_INTS[];

  static int coupon(const uint64_t hash[]);
  static void hash(const void* key, const int keyLen, const uint64_t seed, uint64_t* result);

  static int checkLgK(const int lgK);
  static void checkMemSize(const uint64_t minBytes, const uint64_t capBytes);
  static inline void checkNumStdDev(const int numStdDev);
  static int pair(const int slotNo, const int value);
  static int getLow26(const int coupon);
  static int getValue(const int coupon);
  static double invPow2(const int e);
  static unsigned int ceilingPowerOf2(unsigned int n);
  static unsigned int simpleIntLog2(unsigned int n); // n must be power of 2
  static unsigned int getNumberOfLeadingZeros(uint64_t x);
  static unsigned int numberOfTrailingZeros(unsigned int n);
  static double getRelErr(const bool upperBound, const bool unioned,
                          const int lgConfigK, const int numStdDev);
};

inline int HllUtil::coupon(const uint64_t hash[]) {
  int addr26 = (int) (hash[0] & KEY_MASK_26);
  int lz = getNumberOfLeadingZeros(hash[1]);
  int value = ((lz > 62 ? 62 : lz) + 1); 
  return (value << KEY_BITS_26) | addr26;
}

inline void HllUtil::hash(const void* key, const int keyLen, const uint64_t seed, uint64_t* result) {
  MurmurHash3_x64_128(key, keyLen, DEFAULT_UPDATE_SEED, result);
}

inline double HllUtil::getRelErr(const bool upperBound, const bool unioned,
                          const int lgConfigK, const int numStdDev) {
  return RelativeErrorTables::getRelErr(upperBound, unioned, lgConfigK, numStdDev);
}

inline int HllUtil::checkLgK(const int lgK) {
  if ((lgK >= HllUtil::MIN_LOG_K) && (lgK <= HllUtil::MAX_LOG_K)) { return lgK; }
  std::stringstream ss;
  ss << "Invalid value of k: " << lgK;
  throw std::invalid_argument(ss.str());
}

inline void HllUtil::checkMemSize(const uint64_t minBytes, const uint64_t capBytes) {
  if (capBytes < minBytes) {
    std::stringstream ss;
    ss << "Given destination array is not large enough: " << capBytes;
    throw std::invalid_argument(ss.str());
  }
}

inline void HllUtil::checkNumStdDev(const int numStdDev) {
  if ((numStdDev < 1) || (numStdDev > 3)) {
    throw std::invalid_argument("NumStdDev may not be less than 1 or greater than 3.");
  }
}

inline int HllUtil::pair(const int slotNo, const int value) {
  return (value << HllUtil::KEY_BITS_26) | (slotNo & HllUtil::KEY_MASK_26);
}

inline int HllUtil::getLow26(const int coupon) { return coupon & HllUtil::KEY_MASK_26; }

inline int HllUtil::getValue(const int coupon) { return coupon >> HllUtil::KEY_BITS_26; }

inline double HllUtil::invPow2(const int e) {
  union {
    long long longVal;
    double doubleVal;
  } conv;
  conv.longVal = (1023L - e) << 52;
  return conv.doubleVal;
}

// compute the next highest power of 2 of 32-bit n
// taken from https://graphics.stanford.edu/~seander/bithacks.html
inline unsigned int HllUtil::ceilingPowerOf2(unsigned int n) {
  n--;
  n |= n >> 1;
  n |= n >> 2;
  n |= n >> 4;
  n |= n >> 8;
  n |= n >> 16;
  return n++;
}

inline unsigned int HllUtil::simpleIntLog2(unsigned int n) {
  const unsigned int e = numberOfTrailingZeros(n);
  assert(n == (1U << e));
  return e;
}

// taken from https://graphics.stanford.edu/~seander/bithacks.html
// input is 32-bit word to count zero bits on right
inline unsigned int HllUtil::numberOfTrailingZeros(unsigned int v) {
  unsigned int c;     // c will be the number of zero bits on the right,
                      // so if v is 1101000 (base 2), then c will be 3
  // NOTE: if 0 == v, then c = 31.
  if (v & 0x1) {
    // special case for odd v (assumed to happen half of the time)
    c = 0;
  } else {
    c = 1;
    if ((v & 0xffff) == 0) {  
      v >>= 16;  
      c += 16;
    }
    if ((v & 0xff) == 0) {  
      v >>= 8;  
      c += 8;
    }
    if ((v & 0xf) == 0) {  
      v >>= 4;
      c += 4;
    }
    if ((v & 0x3) == 0) {  
      v >>= 2;
      c += 2;
    }
    c -= v & 0x1;
  }
  return c;	
}

inline unsigned int HllUtil::getNumberOfLeadingZeros(const uint64_t x) {
  if (x == 0)
    return 64;

  // we know at least some 1 bit, so iterate until it's in leftmost position
  // -- making endian assumptions here
  unsigned int n = 0;
  uint64_t val = x;
  while ((val & 0x8000000000000000L) == 0) {
    ++n;
    val <<= 1;
  }
  return n;
}

}
