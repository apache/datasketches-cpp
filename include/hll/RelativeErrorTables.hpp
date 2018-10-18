/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#pragma once

namespace datasketches {

class RelativeErrorTables {
  public:
    /**
     * Return Relative Error for UB or LB for HIP or Non-HIP as a function of numStdDev.
     * @param upperBound true if for upper bound
     * @param oooFlag true if for Non-HIP
     * @param lgK must be between 4 and 12 inclusive
     * @param stdDev must be between 1 and 3 inclusive
     * @return Relative Error for UB or LB for HIP or Non-HIP as a function of numStdDev.
     */
    static double getRelErr(const bool upperBound, const bool oooFlag,
                            const int lgK, const int stdDev);
};

}
