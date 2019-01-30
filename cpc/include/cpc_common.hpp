/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
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
