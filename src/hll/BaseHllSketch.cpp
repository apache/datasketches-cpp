/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "BaseHllSketch.hpp"
#include "MurmurHash3.h"
#include "RelativeErrorTables.hpp"

#include <cmath>
#include <cassert>

namespace datasketches {

int BaseHllSketch::getSerializationVersion() {
  return HllUtil::SER_VER;
}

bool BaseHllSketch::isEstimationMode() const {
  return true;
}

std::ostream& BaseHllSketch::to_string(std::ostream& os) const {
  return to_string(os, true, false, false, false);
}

std::ostream& BaseHllSketch::to_string(std::ostream& os, const bool summary, const bool detail, const bool auxDetail) const {
  return to_string(os, summary, detail, auxDetail, false);
}

int BaseHllSketch::coupon(const uint64_t hash[]) {
  int addr26 = (int) (hash[0] & KEY_MASK_26);
  int lz = getNumberOfLeadingZeros(hash[1]);
  int value = ((lz > 62 ? 62 : lz) + 1); 
  return (value << KEY_BITS_26) | addr26;
}

int BaseHllSketch::getNumberOfLeadingZeros(const uint64_t x) {
  if (x == 0)
    return 64;

  // we know at least some 1 bit, so iterate until it's in leftmost position
  // -- making endian assumptions here
  int n = 0;
  uint64_t val = x;
  while ((val & 0x8000000000000000L) == 0) {
    ++n;
    val <<= 1;
  }
  return n;
}

void BaseHllSketch::update(const std::string datum) {
  if (datum.empty()) { return; }
  uint64_t hashResult[2];
  hash(datum.c_str(), datum.length(), DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(coupon(hashResult));
}

void BaseHllSketch::update(const uint64_t datum) {
  update(&datum, sizeof(datum));
}

void BaseHllSketch::update(const double datum) {
  double d = ((datum == 0.0) ? 0.0 : datum); // canonicalize -0.0, 0.0
  d = (std::isnan(d) ? NAN : d); // canonicalize NaN, although portability to Java not guaranteed
  update(&d, sizeof(d));
}

void BaseHllSketch::update(const void* data, const size_t len) {
  if (data == nullptr) { return; }
  uint64_t hashResult[2];
  hash(data, len, DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(coupon(hashResult));
}

void BaseHllSketch::hash(const void* key, const int keyLen, const uint64_t seed, uint64_t* result) {
  MurmurHash3_x64_128(key, keyLen, DEFAULT_UPDATE_SEED, result);
}

double BaseHllSketch::getRelErr(const bool upperBound, const bool unioned,
                                const int lgConfigK, const int numStdDev) {
  return RelativeErrorTables::getRelErr(upperBound, unioned, lgConfigK, numStdDev);
}


}
