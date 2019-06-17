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

class Hll4Array final : public HllArray {
  public:
    explicit Hll4Array(int lgConfigK);
    explicit Hll4Array(const Hll4Array& that);

    virtual ~Hll4Array();

    virtual Hll4Array* copy() const;

    virtual std::unique_ptr<PairIterator> getIterator() const;
    virtual std::unique_ptr<PairIterator> getAuxIterator() const;

    virtual int getSlot(int slotNo) const final;
    virtual void putSlot(int slotNo, int value) final;

    virtual int getUpdatableSerializationBytes() const;
    virtual int getHllByteArrBytes() const;

    virtual HllSketchImpl* couponUpdate(int coupon) final;

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
