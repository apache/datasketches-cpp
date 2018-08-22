/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include <cmath>

#include "HllUtil.hpp"
#include "CubicInterpolation.hpp"
#include "AbstractCoupons.hpp"

namespace datasketches {

AbstractCoupons::AbstractCoupons(const int lgConfigK, const TgtHllType tgtHllType, const CurMode curMode)
  : HllSketchImpl(lgConfigK, tgtHllType, curMode) {}

AbstractCoupons::~AbstractCoupons() {}

double AbstractCoupons::getCompositeEstimate() { return getEstimate(); }

double AbstractCoupons::getEstimate() {
  const int couponCount = getCouponCount();
  const double est = CubicInterpolation::usingXAndYTables(couponCount);
  return fmax(est, couponCount);
}

double AbstractCoupons::getLowerBound(const int numStdDev) {
  HllUtil::checkNumStdDev(numStdDev);
  const int couponCount = getCouponCount();
  const double est = CubicInterpolation::usingXAndYTables(couponCount);
  const double tmp = est / (1.0 + (numStdDev * HllUtil::COUPON_RSE));
  return fmax(tmp, couponCount);
}

double AbstractCoupons::getUpperBound(const int numStdDev) {
  HllUtil::checkNumStdDev(numStdDev);
  const int couponCount = getCouponCount();
  const double est = CubicInterpolation::usingXAndYTables(couponCount);
  const double tmp = est / (1.0 - (numStdDev * HllUtil::COUPON_RSE));
  return fmax(tmp, couponCount);
}

bool AbstractCoupons::isEmpty() { return getCouponCount() == 0; }

int AbstractCoupons::getUpdatableSerializationBytes() {
  return getMemDataStart() + (4 << getLgCouponArrInts());
}

}
