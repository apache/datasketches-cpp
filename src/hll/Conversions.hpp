/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _CONVERSIONS_HPP_
#define _CONVERSIONS_HPP_

#include "Hll4Array.hpp"
#include "Hll8Array.hpp"

namespace datasketches {

class Conversions {
public:
  static Hll4Array* convertToHll4(HllArray& srcHllArr);
  static Hll8Array* convertToHll8(HllArray& srcHllArr);

private:
  static int curMinAndNum(HllArray& hllArr);
};

}

#endif // _CONVERSIONS_HPP_
