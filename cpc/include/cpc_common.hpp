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

#ifndef CPC_COMMON_HPP_
#define CPC_COMMON_HPP_

#include "MurmurHash3.h"
#include <memory>

namespace datasketches {

static const uint8_t CPC_MIN_LG_K = 4;
static const uint8_t CPC_MAX_LG_K = 26;
static const uint8_t CPC_DEFAULT_LG_K = 11;
static const uint64_t DEFAULT_SEED = 9001;

static uint16_t compute_seed_hash(uint64_t seed) {
  HashState hashes;
  MurmurHash3_x64_128(&seed, sizeof(seed), 0, hashes);
  return hashes.h1 & 0xffff;
}

} /* namespace datasketches */

#endif
