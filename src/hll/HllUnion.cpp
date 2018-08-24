/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "HllUnion.hpp"

#include "HllSketchImpl.hpp"
#include "HllArray.hpp"
#include "HllUtil.hpp"

namespace datasketches {

HllUnion::HllUnion(const int lgMaxK)
  : lgMaxK(HllUtil::checkLgK(lgMaxK)) {
  gadget = new HllSketch(lgMaxK, TgtHllType::HLL_8);
}

HllUnion::HllUnion(HllSketch& sketch)
  : lgMaxK(sketch.getLgConfigK()) {
  TgtHllType tgtHllType = sketch.getTgtHllType();
  if (tgtHllType != TgtHllType::HLL_8) {
    throw std::invalid_argument("HllUnion can only wrap HLL_8 sketches");
  }
  gadget = &sketch;
}

HllUnion::~HllUnion() {
  if (gadget != nullptr) {
    delete gadget;
  }
}

HllSketch* HllUnion::getResult() const {
  return gadget->copyAs(TgtHllType::HLL_4);
}

HllSketch* HllUnion::getResult(TgtHllType tgtHllType) const {
  return gadget->copyAs(tgtHllType);
}

void HllUnion::update(const HllSketch* sketch) {
  unionImpl(sketch->hllSketchImpl, lgMaxK);
}

void HllUnion::update(const HllSketch& sketch) {
  unionImpl(sketch.hllSketchImpl, lgMaxK);
}

void HllUnion::couponUpdate(const int coupon) {
  if (coupon == HllUtil::EMPTY) { return; }
  HllSketchImpl* result = gadget->hllSketchImpl->couponUpdate(coupon);
  if (result != gadget->hllSketchImpl) {
    if (gadget->hllSketchImpl != nullptr) { delete gadget->hllSketchImpl; }
    gadget->hllSketchImpl = result;
  }
}

void HllUnion::serializeCompact(std::ostream& os) const {
  return gadget->serializeCompact(os);
}

void HllUnion::serializeUpdatable(std::ostream& os) const {
  return gadget->serializeUpdatable(os);
}

std::ostream& HllUnion::to_string(std::ostream& os, const bool summary,
                               const bool detail, const bool auxDetail, const bool all) const {
  return gadget->to_string(os, summary, detail, auxDetail, all);
}

double HllUnion::getEstimate() const {
  return gadget->getEstimate();
}

double HllUnion::getCompositeEstimate() const {
  return gadget->getCompositeEstimate();
}

double HllUnion::getLowerBound(const int numStdDev) const {
  return gadget->getLowerBound(numStdDev);
}

double HllUnion::getUpperBound(const int numStdDev) const {
  return gadget->getUpperBound(numStdDev);
}

int HllUnion::getCompactSerializationBytes() const {
  return gadget->getCompactSerializationBytes();
}

int HllUnion::getUpdatableSerializationBytes() const {
  return gadget->getUpdatableSerializationBytes();
}

int HllUnion::getLgConfigK() const {
  return gadget->getLgConfigK();
}

void HllUnion::reset() {
  gadget->reset();
}

bool HllUnion::isCompact() const {
  return gadget->isCompact();
}

bool HllUnion::isEmpty() const {
  return gadget->isEmpty();
}

bool HllUnion::isOutOfOrderFlag() const {
  return gadget->isOutOfOrderFlag();
}

CurMode HllUnion::getCurMode() const {
  return gadget->getCurMode();
}

TgtHllType HllUnion::getTgtHllType() const {
  return TgtHllType::HLL_8;
}

int HllUnion::getMaxSerializationBytes(const int lgK) {
  return HllSketch::getMaxUpdatableSerializationBytes(lgK, TgtHllType::HLL_8);
}

HllSketchImpl* HllUnion::copyOrDownsampleHll(HllSketchImpl* srcImpl, const int tgtLgK) {
  assert(srcImpl->getCurMode() == CurMode::HLL);
  HllArray* src = (HllArray*) srcImpl;
  const int srcLgK = src->getLgConfigK();
  if ((srcLgK <= tgtLgK) && (src->getTgtHllType() == TgtHllType::HLL_8)) {
    return src->copy();
  }
  const int minLgK = ((srcLgK < tgtLgK) ? srcLgK : tgtLgK);
  HllArray* tgtHllArr = HllArray::newHll(minLgK, TgtHllType::HLL_8);
  std::unique_ptr<PairIterator> srcItr = src->getIterator();
  while (srcItr->nextValid()) {
    tgtHllArr->couponUpdate(srcItr->getPair());
  }
  //both of these are required for isomorphism
  tgtHllArr->putHipAccum(src->getHipAccum());
  tgtHllArr->putOutOfOrderFlag(src->isOutOfOrderFlag());
  
  return tgtHllArr;
}

inline HllSketchImpl* HllUnion::leakFreeCouponUpdate(HllSketchImpl* impl, const int coupon) {
  HllSketchImpl* result = impl->couponUpdate(coupon);
  if (result != impl) {
    delete impl;
  }
  return result;
}

void HllUnion::unionImpl(HllSketchImpl* incomingImpl, const int lgMaxK) {
  assert(gadget->hllSketchImpl->getTgtHllType() == TgtHllType::HLL_8);
  HllSketchImpl* srcImpl = incomingImpl; //default
  HllSketchImpl* dstImpl = gadget->hllSketchImpl; //default
  if ((incomingImpl == nullptr) || incomingImpl->isEmpty()) {
    return; // gadget->hllSketchImpl;
  }

  const int hi2bits = (gadget->hllSketchImpl->isEmpty()) ? 3 : gadget->hllSketchImpl->getCurMode();
  const int lo2bits = incomingImpl->getCurMode();

  // TODO: track when we need to free the old gadget

  const int sw = (hi2bits << 2) | lo2bits;
  //System.out.println("SW: " + sw);
  switch (sw) {
    case 0: { //src: LIST, gadget: LIST
      std::unique_ptr<PairIterator> srcItr = srcImpl->getIterator(); //LIST
      while (srcItr->nextValid()) {
        dstImpl = leakFreeCouponUpdate(dstImpl, srcItr->getPair()); //assignment required
      }
      //whichever is True wins:
      dstImpl->putOutOfOrderFlag(dstImpl->isOutOfOrderFlag() | srcImpl->isOutOfOrderFlag());
      // gadget: cleanly updated as needed
      break;
    }
    case 1: { //src: SET, gadget: LIST
      //consider a swap here
      std::unique_ptr<PairIterator> srcItr = srcImpl->getIterator(); //SET
      while (srcItr->nextValid()) {
        dstImpl = leakFreeCouponUpdate(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(true); //SET oooFlag is always true
      // gadget: cleanly updated as needed
      break;
    }
    case 2: { //src: HLL, gadget: LIST
      //swap so that src is gadget-LIST, tgt is HLL
      //use lgMaxK because LIST has effective K of 2^26
      srcImpl = gadget->hllSketchImpl;
      dstImpl = copyOrDownsampleHll(incomingImpl, lgMaxK);
      std::unique_ptr<PairIterator> srcItr = srcImpl->getIterator();
      while (srcItr->nextValid()) {
        dstImpl = leakFreeCouponUpdate(dstImpl, srcItr->getPair()); //assignment required
      }
      //whichever is True wins:
      dstImpl->putOutOfOrderFlag(srcImpl->isOutOfOrderFlag() | dstImpl->isOutOfOrderFlag());
      // gadget: swapped, replacing with new impl
      delete gadget->hllSketchImpl;
      break;
    }
    case 4: { //src: LIST, gadget: SET
      std::unique_ptr<PairIterator> srcItr = srcImpl->getIterator(); //LIST
      while (srcItr->nextValid()) {
        dstImpl = leakFreeCouponUpdate(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(true); //SET oooFlag is always true
      // gadget: cleanly updated as needed
      break;
    }
    case 5: { //src: SET, gadget: SET
      std::unique_ptr<PairIterator> srcItr = srcImpl->getIterator(); //SET
      while (srcItr->nextValid()) {
        dstImpl = leakFreeCouponUpdate(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(true); //SET oooFlag is always true
      // gadget: cleanly updated as needed
      break;
    }
    case 6: { //src: HLL, gadget: SET
      //swap so that src is gadget-SET, tgt is HLL
      //use lgMaxK because LIST has effective K of 2^26
      srcImpl = gadget->hllSketchImpl;
      dstImpl = copyOrDownsampleHll(incomingImpl, lgMaxK);
      std::unique_ptr<PairIterator> srcItr = srcImpl->getIterator(); //LIST
      assert(dstImpl->getCurMode() == HLL);
      while (srcItr->nextValid()) {
        dstImpl = leakFreeCouponUpdate(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(true); //merging SET into non-empty HLL -> true
      // gadget: swapped, replacing with new impl
      delete gadget->hllSketchImpl;
      break;
    }
    case 8: { //src: LIST, gadget: HLL
      assert(dstImpl->getCurMode() == HLL);
      std::unique_ptr<PairIterator> srcItr = srcImpl->getIterator(); //LIST
      while (srcItr->nextValid()) {
        dstImpl = leakFreeCouponUpdate(dstImpl, srcItr->getPair()); //assignment required
      }
      //whichever is True wins:
      dstImpl->putOutOfOrderFlag(dstImpl->isOutOfOrderFlag() | srcImpl->isOutOfOrderFlag());
      // gadget: should remain unchanged
      assert(dstImpl == gadget->hllSketchImpl); // should not have changed from HLL
      break;
    }
    case 9: { //src: SET, gadget: HLL
      assert(dstImpl->getCurMode() == HLL);
      std::unique_ptr<PairIterator> srcItr = srcImpl->getIterator(); //SET
      while (srcItr->nextValid()) {
        dstImpl = leakFreeCouponUpdate(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(true); //merging SET into existing HLL -> true
      // gadget: should remain unchanged
      assert(dstImpl == gadget->hllSketchImpl); // should not have changed from HLL
      break;
    }
    case 10: { //src: HLL, gadget: HLL
      const int srcLgK = srcImpl->getLgConfigK();
      const int dstLgK = dstImpl->getLgConfigK();
      const int minLgK = ((srcLgK < dstLgK) ? srcLgK : dstLgK);
      if ((srcLgK < dstLgK) || (dstImpl->getTgtHllType() != HLL_8)) {
        dstImpl = copyOrDownsampleHll(dstImpl, minLgK);
        // always replaces gadget
        delete gadget->hllSketchImpl;
      }
      std::unique_ptr<PairIterator> srcItr = srcImpl->getIterator(); //HLL
      while (srcItr->nextValid()) {
        dstImpl = leakFreeCouponUpdate(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(true); //union of two HLL modes is always true
      // gadget: replaced if copied/downampled, otherwise should be unchanged
      break;
    }
    case 12: { //src: LIST, gadget: empty
      std::unique_ptr<PairIterator> srcItr = srcImpl->getIterator(); //LIST
      while (srcItr->nextValid()) {
        dstImpl = leakFreeCouponUpdate(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(srcImpl->isOutOfOrderFlag()); //whatever source is
      // gadget: cleanly updated as needed
      break;
    }
    case 13: { //src: SET, gadget: empty
      std::unique_ptr<PairIterator> srcItr = srcImpl->getIterator(); //SET
      while (srcItr->nextValid()) {
        dstImpl = leakFreeCouponUpdate(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(true); //SET oooFlag is always true
      // gadget: cleanly updated as needed
      break;
    }
    case 14: { //src: HLL, gadget: empty
      dstImpl = copyOrDownsampleHll(srcImpl, lgMaxK);
      dstImpl->putOutOfOrderFlag(srcImpl->isOutOfOrderFlag()); //whatever source is.
      // gadget: always replaced with copied/downsampled sketch
      delete gadget->hllSketchImpl;
      break;
    }
  }
  
  gadget->hllSketchImpl = dstImpl;
}

}
