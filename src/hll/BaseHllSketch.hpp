/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#pragma once

#include "HllUtil.hpp"

namespace datasketches {

enum TgtHllType {
    HLL_4 = 0,
    HLL_8
};

class BaseHllSketch {
  public:
    static const int DEFAULT_K = 16;

    explicit BaseHllSketch() {}
    virtual ~BaseHllSketch() {}

    bool isEstimationMode() const;

    virtual double getCompositeEstimate() const = 0;
    virtual double getEstimate() const = 0;
    virtual double getLowerBound(int numStdDev) const = 0;
    virtual double getUpperBound(int numStdDev) const = 0;

    virtual TgtHllType getTgtHllType() const = 0;
    virtual int getLgConfigK() const = 0;
    virtual bool isEmpty() const = 0;
    virtual bool isCompact() const = 0;

    static int getSerializationVersion();
    virtual int getUpdatableSerializationBytes() const = 0;
    virtual int getCompactSerializationBytes() const = 0;

    virtual void reset() = 0;

    static double getRelErr(const bool upperBound, const bool unioned,
                            const int lgConfigK, const int numStdDev);

    virtual void serializeCompact(std::ostream& os) const = 0;
    virtual void serializeUpdatable(std::ostream& os) const = 0;

    virtual std::ostream& to_string(std::ostream& os) const;
    virtual std::ostream& to_string(std::ostream& os, const bool summary, const bool detail, const bool auxDetail) const;
    virtual std::ostream& to_string(std::ostream& os, const bool summary,
                                    const bool detail, const bool auxDetail, const bool all) const = 0;

    // TODO: is this portable?
    void update(const std::string datum);
    void update(const uint64_t datum);
    void update(const double datum);
    void update(const void* data, const size_t len);


  protected:
    virtual bool isOutOfOrderFlag() const = 0;

    virtual void couponUpdate(int coupon) = 0;

    virtual enum CurMode getCurMode() const = 0;

    static const uint64_t DEFAULT_UPDATE_SEED = 9001L;
    static const int KEY_BITS_26 = 26;
    static const int KEY_MASK_26 = (1 << KEY_BITS_26) - 1;

  private:

    static void hash(const void* key, const int keyLen, const uint64_t seed, uint64_t* out);

    static int coupon(const uint64_t hash[]);

    static int getNumberOfLeadingZeros(const uint64_t x);
};

}
