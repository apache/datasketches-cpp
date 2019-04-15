/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _HLLUNION_INTERNAL_HPP_
#define _HLLUNION_INTERNAL_HPP_

#include "HllUnion.hpp"

#include "HllSketchImpl.hpp"
#include "HllArray.hpp"
#include "HllUtil.hpp"

#include <stdexcept>
#include <string>

namespace datasketches {

template<typename A>
HllUnion<A> HllUnion<A>::newInstance(const int lgMaxK) {
  //return std::unique_ptr<HllUnion>(new HllUnionPvt(lgMaxK));
  HllUnion<A> u(lgMaxK);
  return u;
}

template<typename A>
HllUnion<A> HllUnion<A>::deserialize(std::istream& is) {
  return HllUnionPvt<A>::deserialize(is);
}

template<typename A>
HllUnion<A> HllUnion<A>::deserialize(const void* bytes, size_t len) {
  return HllUnionPvt<A>::deserialize(bytes, len);
}

template<typename A>
HllUnion<A>::~HllUnion() {}

template<typename A>
static std::ostream& operator<<(std::ostream& os, HllUnion<A>& hllUnion) {
  return hllUnion.to_string(os, true, true, false, false);
}

/*
static std::ostream& operator<<(std::ostream& os, hll_union& hllUnion) {
  return hllUnion->to_string(os, true, true, false, false);
}
*/

template<typename A>
HllUnionPvt<A>::HllUnionPvt(const int lgMaxK)
  : lgMaxK(HllUtil::checkLgK(lgMaxK)) {
  gadget = HllSketch<A>::newInstance(lgMaxK, TgtHllType::HLL_8);
}

template<typename A>
HllUnionPvt<A>::HllUnionPvt(HllSketchPvt<A>& sketch)
  : lgMaxK(sketch->getLgConfigK()) {
  TgtHllType tgtHllType = sketch->getTgtHllType();
  if (tgtHllType != TgtHllType::HLL_8) {
    throw std::invalid_argument("HllUnion can only wrap HLL_8 sketches");
  }
  std::swap(sketch, gadget);
}

template<typename A>
HllUnionPvt<A>::HllUnionPvt(const HllUnionPvt<A>& other) :
  lgMaxK(static_cast<const HllUnionPvt<A>>(other).lgMaxK),
  gadget(static_cast<const HllUnionPvt<A>>(other).gadget.get()->copy())
{}

template<typename A>
HllUnionPvt<A>& HllUnionPvt<A>::operator=(HllUnionPvt<A>& other) {
  lgMaxK = other.lgMaxK;
  std::swap(gadget, other.gadget);
  return *this;
}

template<typename A>
HllUnionPvt<A>::~HllUnionPvt() {}

template<typename A>
HllUnionPvt<A> HllUnionPvt<A>::deserialize(const void* bytes, size_t len) {
  HllSketchPvt<A> sk(HllSketchPvt<A>::deserialize(bytes, len));
  if (sk == nullptr) { return nullptr; }
  // we're using the sketch's lgConfigK to initialize the union so
  // we can initialize the Union with it as long as it's HLL_8.
  HllUnionPvt* hllUnion = AllocHllUnion::allocate(1);
  if (sk->getTgtHllType() == HLL_8) {
    //hllUnion = new HllUnionPvt(std::move(sk));
    AllocHllUnion::construct(hllUnion, std::move(sk));
  } else {
    //hllUnion = new HllUnionPvt(sk->getLgConfigK());
    AllocHllUnion::construct(hllUnion, sk->getLgConfigK());
    hllUnion->update(*sk);
  }
  //return std::unique_ptr<HllUnionPvt>(hllUnion);
  return hllUnion;
}

template<typename A>
HllUnionPvt<A> HllUnionPvt<A>::deserialize(std::istream& is) {
  HllSketchPvt<A> sk(HllSketchPvt<A>::deserialize(is));
  if (sk == nullptr) { return nullptr; }
  // we're using the sketch's lgConfigK to initialize the union so
  // we can initialize the Union with it as long as it's HLL_8.
  HllUnionPvt* hllUnion = AllocHllUnion::allocate(1);
  if (sk->getTgtHllType() == HLL_8) {
    //hllUnion = new HllUnionPvt(std::move(sk));
    AllocHllUnion::construct(hllUnion, std::move(sk));
  } else {
    //hllUnion = new HllUnionPvt(sk->getLgConfigK());
    AllocHllUnion::construct(hllUnion, sk->getLgConfigK());
    hllUnion->update(*sk);
  }
  //return std::unique_ptr<HllUnionPvt>(hllUnion);
  return hllUnion;
}

template<typename A>
HllSketch<A> HllUnionPvt<A>::getResult(TgtHllType tgtHllType) const {
  return gadget->copyAs(tgtHllType);
}

template<typename A>
void HllUnionPvt<A>::update(const HllSketch<A>& sketch) {
  unionImpl(static_cast<const HllSketchPvt<A>&>(sketch).hllSketchImpl, lgMaxK);
}

template<typename A>
void HllUnionPvt<A>::update(const std::string& datum) {
  gadget->update(datum);
}

template<typename A>
void HllUnionPvt<A>::update(const uint64_t datum) {
  gadget->update(datum);
}

template<typename A>
void HllUnionPvt<A>::update(const uint32_t datum) {
  gadget->update(datum);
}

template<typename A>
void HllUnionPvt<A>::update(const uint16_t datum) {
  gadget->update(datum);
}

template<typename A>
void HllUnionPvt<A>::update(const uint8_t datum) {
  gadget->update(datum);
}

template<typename A>
void HllUnionPvt<A>::update(const int64_t datum) {
  gadget->update(datum);
}

template<typename A>
void HllUnionPvt<A>::update(const int32_t datum) {
  gadget->update(datum);
}

template<typename A>
void HllUnionPvt<A>::update(const int16_t datum) {
  gadget->update(datum);
}

template<typename A>
void HllUnionPvt<A>::update(const int8_t datum) {
  gadget->update(datum);
}

template<typename A>
void HllUnionPvt<A>::update(const double datum) {
  gadget->update(datum);
}

template<typename A>
void HllUnionPvt<A>::update(const float datum) {
  gadget->update(datum);
}

template<typename A>
void HllUnionPvt<A>::update(const void* data, const size_t lengthBytes) {
  gadget->update(data, lengthBytes);
}

template<typename A>
void HllUnionPvt<A>::couponUpdate(const int coupon) {
  if (coupon == HllUtil::EMPTY) { return; }
  HllSketchImpl* result = gadget->hllSketchImpl->couponUpdate(coupon);
  if (result != gadget->hllSketchImpl) {
    if (gadget->hllSketchImpl != nullptr) { delete gadget->hllSketchImpl; }
    gadget->hllSketchImpl = result;
  }
}

template<typename A>
std::pair<std::unique_ptr<uint8_t[]>, const size_t> HllUnionPvt<A>::serializeCompact() const {
  return gadget->serializeCompact();
}

template<typename A>
std::pair<std::unique_ptr<uint8_t[]>, const size_t> HllUnionPvt<A>::serializeUpdatable() const {
  return gadget->serializeUpdatable();
}

template<typename A>
void HllUnionPvt<A>::serializeCompact(std::ostream& os) const {
  return gadget->serializeCompact(os);
}

template<typename A>
void HllUnionPvt<A>::serializeUpdatable(std::ostream& os) const {
  return gadget->serializeUpdatable(os);
}

template<typename A>
std::ostream& HllUnionPvt<A>::to_string(std::ostream& os, const bool summary,
                                  const bool detail, const bool auxDetail, const bool all) const {
  return gadget->to_string(os, summary, detail, auxDetail, all);
}

template<typename A>
std::string HllUnionPvt<A>::to_string(const bool summary, const bool detail,
                                   const bool auxDetail, const bool all) const {
  return gadget->to_string(summary, detail, auxDetail, all);
}

template<typename A>
double HllUnionPvt<A>::getEstimate() const {
  return gadget->getEstimate();
}

template<typename A>
double HllUnionPvt<A>::getCompositeEstimate() const {
  return gadget->getCompositeEstimate();
}

template<typename A>
double HllUnionPvt<A>::getLowerBound(const int numStdDev) const {
  return gadget->getLowerBound(numStdDev);
}

template<typename A>
double HllUnionPvt<A>::getUpperBound(const int numStdDev) const {
  return gadget->getUpperBound(numStdDev);
}

template<typename A>
int HllUnionPvt<A>::getCompactSerializationBytes() const {
  return gadget->getCompactSerializationBytes();
}

template<typename A>
int HllUnionPvt<A>::getUpdatableSerializationBytes() const {
  return gadget->getUpdatableSerializationBytes();
}

template<typename A>
int HllUnionPvt<A>::getLgConfigK() const {
  return gadget->getLgConfigK();
}

template<typename A>
void HllUnionPvt<A>::reset() {
  gadget->reset();
}

template<typename A>
bool HllUnionPvt<A>::isCompact() const {
  return gadget->isCompact();
}

template<typename A>
bool HllUnionPvt<A>::isEmpty() const {
  return gadget->isEmpty();
}

template<typename A>
bool HllUnionPvt<A>::isOutOfOrderFlag() const {
  return gadget->isOutOfOrderFlag();
}

template<typename A>
CurMode HllUnionPvt<A>::getCurrentMode() const {
  return gadget->getCurrentMode();
}

template<typename A>
bool HllUnionPvt<A>::isEstimationMode() const {
  return gadget->isEstimationMode();
}

template<typename A>
int HllUnionPvt<A>::getSerializationVersion() const {
  return HllUtil::SER_VER;
}

template<typename A>
TgtHllType HllUnionPvt<A>::getTgtHllType() const {
  return TgtHllType::HLL_8;
}

template<typename A>
int HllUnion<A>::getMaxSerializationBytes(const int lgK) {
  return HllSketch<A>::getMaxUpdatableSerializationBytes(lgK, TgtHllType::HLL_8);
}

template<typename A>
double HllUnion<A>::getRelErr(const bool upperBound, const bool unioned,
                           const int lgConfigK, const int numStdDev) {
  return HllUtil::getRelErr(upperBound, unioned, lgConfigK, numStdDev);
}

template<typename A>
HllSketchImpl* HllUnionPvt<A>::copyOrDownsampleHll(HllSketchImpl* srcImpl, const int tgtLgK) {
  if (srcImpl->getCurMode() != CurMode::HLL) {
    throw std::logic_error("Attempt to downsample non-HLL sketch");
  }
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

template<typename A>
inline HllSketchImpl* HllUnionPvt<A>::leakFreeCouponUpdate(HllSketchImpl* impl, const int coupon) {
  HllSketchImpl* result = impl->couponUpdate(coupon);
  if (result != impl) {
    delete impl;
  }
  return result;
}

template<typename A>
void HllUnionPvt<A>::unionImpl(HllSketchImpl* incomingImpl, const int lgMaxK) {
  if (gadget->hllSketchImpl->getTgtHllType() != TgtHllType::HLL_8) {
    throw std::logic_error("Must call unionImpl() with HLL_8 input");
  }
  HllSketchImpl* srcImpl = incomingImpl; //default
  HllSketchImpl* dstImpl = gadget->hllSketchImpl; //default
  if ((incomingImpl == nullptr) || incomingImpl->isEmpty()) {
    return; // gadget->hllSketchImpl;
  }

  const int hi2bits = (gadget->hllSketchImpl->isEmpty()) ? 3 : gadget->hllSketchImpl->getCurMode();
  const int lo2bits = incomingImpl->getCurMode();

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
      if (dstImpl->getCurMode() != HLL) {
        throw std::logic_error("dstImpl must be in HLL mode");
      }
      while (srcItr->nextValid()) {
        dstImpl = leakFreeCouponUpdate(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(true); //merging SET into non-empty HLL -> true
      // gadget: swapped, replacing with new impl
      delete gadget->hllSketchImpl;
      break;
    }
    case 8: { //src: LIST, gadget: HLL
      if (dstImpl->getCurMode() != HLL) {
        throw std::logic_error("dstImpl must be in HLL mode");
      }
      std::unique_ptr<PairIterator> srcItr = srcImpl->getIterator(); //LIST
      while (srcItr->nextValid()) {
        dstImpl = leakFreeCouponUpdate(dstImpl, srcItr->getPair()); //assignment required
      }
      //whichever is True wins:
      dstImpl->putOutOfOrderFlag(dstImpl->isOutOfOrderFlag() | srcImpl->isOutOfOrderFlag());
      // gadget: should remain unchanged
      if (dstImpl != gadget->hllSketchImpl) {
        // should not have changed from HLL
        throw std::logic_error("dstImpl unepxectedly changed from gadget");
      } 
      break;
    }
    case 9: { //src: SET, gadget: HLL
      if (dstImpl->getCurMode() != HLL) {
        throw std::logic_error("dstImpl must be in HLL mode");
      }
      std::unique_ptr<PairIterator> srcItr = srcImpl->getIterator(); //SET
      while (srcItr->nextValid()) {
        dstImpl = leakFreeCouponUpdate(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(true); //merging SET into existing HLL -> true
      // gadget: should remain unchanged
      if (dstImpl != gadget->hllSketchImpl) {
        // should not have changed from HLL
        throw std::logic_error("dstImpl unepxectedly changed from gadget");
      } 
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

#endif // _HLLUNION_INTERNAL_HPP_