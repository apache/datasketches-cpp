/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _CUBICINTERPOLATION_HPP_
#define _CUBICINTERPOLATION_HPP_

namespace datasketches {

class CubicInterpolation {
  public:
    static double usingXAndYTables(const double xArr[], const double yArr[],
                                   int len, double x);

    static double usingXAndYTables(double x);

    static double usingXArrAndYStride(const double xArr[], const int xArrLen,
                                      double yStride, double x);
};

}

#endif /* _CUBICINTERPOLATION_HPP_ */