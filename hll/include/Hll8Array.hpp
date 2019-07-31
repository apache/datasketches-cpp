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

#ifndef _HLL8ARRAY_HPP_
#define _HLL8ARRAY_HPP_

#include "HllArray.hpp"
#include "HllPairIterator.hpp"

namespace datasketches {

template<typename A>
class Hll8Iterator;

template<typename A>
class Hll8Array final : public HllArray<A> {
  public:
    explicit Hll8Array(int lgConfigK, bool startFullSize);
    explicit Hll8Array(const Hll8Array& that);

    virtual ~Hll8Array();
    virtual std::function<void(HllSketchImpl<A>*)> get_deleter() const;

    virtual Hll8Array<A>* copy() const;

    virtual PairIterator_with_deleter<A> getIterator() const;

    virtual int getSlot(int slotNo) const final;
    virtual void putSlot(int slotNo, int value) final;

    virtual HllSketchImpl<A>* couponUpdate(int coupon) final;

    virtual int getHllByteArrBytes() const;

  protected:
    friend class Hll8Iterator<A>;
};

template<typename A>
class Hll8Iterator : public HllPairIterator<A> {
  public:
    Hll8Iterator(const Hll8Array<A>& array, int lengthPairs);
    virtual int value();

    virtual ~Hll8Iterator();

  private:
    const Hll8Array<A>& hllArray;
};

}

#endif /* _HLL8ARRAY_HPP_ */
