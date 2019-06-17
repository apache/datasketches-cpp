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

#ifndef _COUPONHASHSET_HPP_
#define _COUPONHASHSET_HPP_

#include "CouponList.hpp"

namespace datasketches {

class CouponHashSet final : public CouponList {
  public:
    static CouponHashSet* newSet(const void* bytes, size_t len);
    static CouponHashSet* newSet(std::istream& is);

  protected:
    explicit CouponHashSet(int lgConfigK, TgtHllType tgtHllType);
    explicit CouponHashSet(const CouponHashSet& that);
    explicit CouponHashSet(const CouponHashSet& that, TgtHllType tgtHllType);
    
    virtual ~CouponHashSet();

    virtual CouponHashSet* copy() const;
    virtual CouponHashSet* copyAs(TgtHllType tgtHllType) const;

    virtual HllSketchImpl* couponUpdate(int coupon);

    virtual int getMemDataStart() const;
    virtual int getPreInts() const;

    friend class CouponList; // so it can access fields declared in CouponList

  private:
    bool checkGrowOrPromote();
    void growHashSet(int srcLgCoupArrSize, int tgtLgCoupArrSize);
};

}

#endif /* _COUPONHASHSET_HPP_ */