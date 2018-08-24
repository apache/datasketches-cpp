/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#pragma once

#include <cassert>
#include <cmath>
#include <exception>
#include <string>
#include <sstream>

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

  static int checkLgK(const int lgK);
  static void checkMemSize(const uint64_t minBytes, const uint64_t capBytes);
  static inline void checkNumStdDev(const int numStdDev);
  static int pair(const int slotNo, const int value);
  static int getLow26(const int coupon);
  static int getValue(const int coupon);
  static double invPow2(const int e);
};

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

}
