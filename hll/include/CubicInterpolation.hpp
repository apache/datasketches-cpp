/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#pragma once

namespace datasketches {

class CubicInterpolation {
  public:
    static double usingXAndYTables(const double xArr[], const double yArr[],
                                   const int len, const double x);

    static double usingXAndYTables(const double x);

    static double usingXArrAndYStride(const double xArr[], const int xArrLen,
                                      const double yStride, const double x);
};

}
