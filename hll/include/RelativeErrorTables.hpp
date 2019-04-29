/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _RELATIVEERRORTABLES_HPP_
#define _RELATIVEERRORTABLES_HPP_

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
    static double getRelErr(bool upperBound, bool oooFlag,
                            int lgK, int stdDev);
};

}

//#include "RelativeErrorTables-internal.hpp"

#endif /* _RELATIVEERRORTABLES_HPP_ */
