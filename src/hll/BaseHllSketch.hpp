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

    bool isEstimationMode();

    virtual double getCompositeEstimate() = 0;
    virtual double getEstimate() = 0;
    virtual double getLowerBound(int numStdDev) = 0;
    virtual double getUpperBound(int numStdDev) = 0;

    virtual TgtHllType getTgtHllType() = 0;

    virtual int getLgConfigK() = 0;

    virtual bool isEmpty() = 0;

    virtual bool isCompact() = 0;

    static int getSerializationVersion();

    //static int getSerializationVersion(BaseHllSketch sketch);

    virtual int getUpdatableSerializationBytes() = 0;

    virtual int getCompactSerializationBytes() = 0;

    virtual void reset() = 0;

    double getRelErr(const bool upperBound, const bool unioned,
                     const int lgConfigK, const int numStdDev);

    //virtual char* toCompactByteArray();

    //virtual char* toUpdatableByteArray();

    virtual std::ostream& to_string(std::ostream& os);
    virtual std::ostream& to_string(std::ostream& os, const bool summary, const bool detail, const bool auxDetail);
    virtual std::ostream& to_string(std::ostream& os, const bool summary,
                                    const bool detail, const bool auxDetail, const bool all) = 0;

    // TODO: is this portable?
    void update(const std::string datum);

    void update(const uint64_t datum);

    void update(const double datum);

    void update(const void* data, const size_t len);


  protected:
    virtual bool isOutOfOrderFlag() = 0;

    virtual void couponUpdate(int coupon) = 0;

    virtual enum CurMode getCurMode() = 0;

    static const uint64_t DEFAULT_UPDATE_SEED = 9001L;
    static const int KEY_BITS_26 = 26;
    static const int KEY_MASK_26 = (1 << KEY_BITS_26) - 1;

  private:

    static void hash(const void* key, const int keyLen, const uint64_t seed, uint64_t* out);

    static int coupon(const uint64_t hash[]);

    static int getNumberOfLeadingZeros(const uint64_t x);
};

}
