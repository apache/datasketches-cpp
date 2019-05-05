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

template<typename A>
class Hll4Iterator;

template<typename A>
class Hll4Array final : public HllArray<A> {
  public:
    explicit Hll4Array(int lgConfigK);
    explicit Hll4Array(const Hll4Array<A>& that);

    virtual ~Hll4Array();
    virtual std::function<void(HllSketchImpl<A>*)> get_deleter() const;

    virtual Hll4Array* copy() const;

    //virtual std::unique_ptr<PairIterator<A>> getIterator() const;
    //virtual std::unique_ptr<PairIterator<A>> getAuxIterator() const;
    virtual PairIterator_with_deleter<A> getIterator() const;
    virtual PairIterator_with_deleter<A> getAuxIterator() const;

    virtual int getSlot(int slotNo) const final;
    virtual void putSlot(int slotNo, int value) final;

    virtual int getUpdatableSerializationBytes() const;
    virtual int getHllByteArrBytes() const;

    virtual HllSketchImpl<A>* couponUpdate(int coupon) final;

    virtual AuxHashMap<A>* getAuxHashMap() const;
    // does *not* delete old map if overwriting
    void putAuxHashMap(AuxHashMap<A>* auxHashMap);

  protected:
    void internalHll4Update(int slotNo, int newVal);
    void shiftToBiggerCurMin();

    AuxHashMap<A>* auxHashMap;

    friend class Hll4Iterator<A>;
};

template<typename A>
class Hll4Iterator : public HllPairIterator<A> {
  public:
    Hll4Iterator(const Hll4Array<A>& array, int lengthPairs);
    virtual int value();

    virtual ~Hll4Iterator();

  private:
    const Hll4Array<A>& hllArray;
};

}

//#include "Hll4Array-internal.hpp"

#endif /* _HLL4ARRAY_HPP_ */
