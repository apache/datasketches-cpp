/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#pragma once

#include "BaseHllSketch.hpp"
#include "PairIterator.hpp"
#include "HllSketchImpl.hpp"

#include <memory>
#include <iostream>

namespace datasketches {

class HllSketchImpl;

class HllSketch : public BaseHllSketch {
  public:
    explicit HllSketch(const int lgConfigK);
    explicit HllSketch(const int lgConfigK, const TgtHllType tgtHllType);
    ~HllSketch();

    HllSketch* copy() const;
    HllSketch* copyAs(const TgtHllType tgtHllType) const;

    void reset();

    virtual void serializeCompact(std::ostream& os) const;
    virtual void serializeUpdatable(std::ostream& os) const;

    virtual std::ostream& to_string(std::ostream& os, const bool summary,
                                    const bool detail, const bool auxDetail, const bool all) const;

    double getEstimate() const;
    double getCompositeEstimate() const;
    double getLowerBound(int numStdDev) const;
    double getUpperBound(int numStdDev) const;

    int getLgConfigK() const;
    TgtHllType getTgtHllType() const;
    bool isOutOfOrderFlag() const;

    bool isCompact() const;
    bool isEmpty() const;

    int getUpdatableSerializationBytes() const;
    int getCompactSerializationBytes() const;

    /**
     * Returns the maximum size in bytes that this sketch can grow to given lgConfigK.
    * However, for the HLL_4 sketch type, this value can be exceeded in extremely rare cases.
    * If exceeded, it will be larger by only a few percent.
    *
    * @param lgConfigK The Log2 of K for the target HLL sketch. This value must be
    * between 4 and 21 inclusively.
    * @param tgtHllType the desired Hll type
    * @return the maximum size in bytes that this sketch can grow to.
    */
    static int getMaxUpdatableSerializationBytes(const int lgK, TgtHllType tgtHllType);

  protected:
    HllSketchImpl* hllSketchImpl;

    virtual std::unique_ptr<PairIterator> getIterator() const;

    CurMode getCurMode() const;

    // copy constructors
    HllSketch(const HllSketch& that);
    HllSketch(HllSketchImpl* that);

    virtual void couponUpdate(int coupon);

    std::string typeAsString() const;
    std::string modeAsString() const;

    friend class HllUnion;
};

std::ostream& operator<<(std::ostream& os, HllSketch& sketch);
}
