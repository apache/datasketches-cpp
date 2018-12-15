/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _COMPOSITEINTERPOLATIONXTABLE_HPP_
#define _COMPOSITEINTERPOLATIONXTABLE_HPP_

namespace datasketches {

class CompositeInterpolationXTable {
  public:
    static const int get_y_stride(int logK);

    static const double* const get_x_arr(int logK);
    static const int get_x_arr_length(int logK);
};

}

#endif /* _COMPOSITEINTERPOLATIONXTABLE_HPP_ */