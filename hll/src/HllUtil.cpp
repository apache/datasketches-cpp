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

#include "HllUtil.hpp"

namespace datasketches {

const double HllUtil<>::HLL_HIP_RSE_FACTOR = sqrt(log(2.0)); // 0.8325546
const double HllUtil<>::HLL_NON_HIP_RSE_FACTOR = sqrt((3.0 * log(2.0)) - 1.0); // 1.03896
const double HllUtil<>::COUPON_RSE_FACTOR = 0.409;
const double HllUtil<>::COUPON_RSE = COUPON_RSE_FACTOR / (1 << 13);

const int HllUtil<>::LG_AUX_ARR_INTS[] = {
      0, 2, 2, 2, 2, 2, 2, 3, 3, 3,   // 0 - 9
      4, 4, 5, 5, 6, 7, 8, 9, 10, 11, // 10-19
      12, 13, 14, 15, 16, 17, 18      // 20-26
      };
      
}