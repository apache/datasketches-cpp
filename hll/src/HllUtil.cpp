/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "HllUtil.hpp"

namespace datasketches {

const double HllUtil::HLL_HIP_RSE_FACTOR = sqrt(log(2.0)); // 0.8325546
const double HllUtil::HLL_NON_HIP_RSE_FACTOR = sqrt((3.0 * log(2.0)) - 1.0); // 1.03896
const double HllUtil::COUPON_RSE_FACTOR = 0.409;
const double HllUtil::COUPON_RSE = COUPON_RSE_FACTOR / (1 << 13);

const int HllUtil::LG_AUX_ARR_INTS[] = {
      0, 2, 2, 2, 2, 2, 2, 3, 3, 3,   // 0 - 9
      4, 4, 5, 5, 6, 7, 8, 9, 10, 11, // 10-19
      12, 13, 14, 15, 16, 17, 18      // 20-26
      };
      
}