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
    explicit Hll4Array(int lgConfigK, bool startFullSize);
    explicit Hll4Array(const Hll4Array<A>& that);

    virtual ~Hll4Array();
    virtual std::function<void(HllSketchImpl<A>*)> get_deleter() const;

    virtual Hll4Array* copy() const;

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

#endif /* _HLL4ARRAY_HPP_ */
