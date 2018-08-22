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
    explicit Hll4Array(Hll4Array& that);

    virtual ~Hll4Array();

    virtual Hll4Array* copy();

    virtual std::unique_ptr<PairIterator> getIterator();
    virtual std::unique_ptr<PairIterator> getAuxIterator();

    virtual int getSlot(const int slotNo);
    virtual void putSlot(const int slotNo, const int value);

    virtual int getHllByteArrBytes();

    virtual HllSketchImpl* couponUpdate(const int coupon);

    virtual AuxHashMap* getAuxHashMap();
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
    Hll4Iterator(Hll4Array& array, const int lengthPairs);
    virtual int value();

    virtual ~Hll4Iterator();

  private:
    Hll4Array& hllArray;
};

}
