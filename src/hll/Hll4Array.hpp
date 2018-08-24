/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#pragma once

#include "HllPairIterator.hpp"
#include "AuxHashMap.hpp"
#include "HllArray.hpp"

namespace datasketches {

class Hll4Array : public HllArray {
  public:
    explicit Hll4Array(const int lgConfigK);
    explicit Hll4Array(const Hll4Array& that);

    virtual ~Hll4Array();

    virtual Hll4Array* copy() const;

    virtual std::unique_ptr<PairIterator> getIterator() const;
    virtual std::unique_ptr<PairIterator> getAuxIterator() const;

    virtual int getSlot(const int slotNo) const;
    virtual void putSlot(const int slotNo, const int value);

    virtual int getHllByteArrBytes() const;

    virtual HllSketchImpl* couponUpdate(const int coupon);

    virtual AuxHashMap* getAuxHashMap() const;
    // does *not* delete old map if overwriting
    void putAuxHashMap(AuxHashMap* auxHashMap);

  protected:
    void internalHll4Update(const int slotNo, const int newVal);
    void shiftToBiggerCurMin();

    AuxHashMap* auxHashMap;

    friend class Hll4Iterator;
};

class Hll4Iterator : public HllPairIterator {
  public:
    Hll4Iterator(const Hll4Array& array, const int lengthPairs);
    virtual int value();

    virtual ~Hll4Iterator();

  private:
    const Hll4Array& hllArray;
};

}
