/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _HLL4ARRAY_HPP_
#define _HLL4ARRAY_HPP_

#include "HllPairIterator.hpp"
#include "AuxHashMap.hpp"
#include "HllArray.hpp"

namespace datasketches {

class Hll4Array : public HllArray {
  public:
    explicit Hll4Array(int lgConfigK);
    explicit Hll4Array(const Hll4Array& that);

    virtual ~Hll4Array();

    virtual Hll4Array* copy() const;

    virtual std::unique_ptr<PairIterator> getIterator() const;
    virtual std::unique_ptr<PairIterator> getAuxIterator() const;

    virtual int getSlot(int slotNo) const;
    virtual void putSlot(int slotNo, int value);

    virtual int getUpdatableSerializationBytes() const;
    virtual int getHllByteArrBytes() const;

    virtual HllSketchImpl* couponUpdate(int coupon);

    virtual AuxHashMap* getAuxHashMap() const;
    // does *not* delete old map if overwriting
    void putAuxHashMap(AuxHashMap* auxHashMap);

  protected:
    void internalHll4Update(int slotNo, int newVal);
    void shiftToBiggerCurMin();

    AuxHashMap* auxHashMap;

    friend class Hll4Iterator;
};

class Hll4Iterator : public HllPairIterator {
  public:
    Hll4Iterator(const Hll4Array& array, int lengthPairs);
    virtual int value();

    virtual ~Hll4Iterator();

  private:
    const Hll4Array& hllArray;
};

}

#endif /* _HLL4ARRAY_HPP_ */
