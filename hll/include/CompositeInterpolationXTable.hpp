/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#pragma once

namespace datasketches {

class CompositeInterpolationXTable {
  public:
    static const int get_y_stride(const int logK);

    static const double* const get_x_arr(const int logK);
    static const int get_x_arr_length(const int logK);
};

}
