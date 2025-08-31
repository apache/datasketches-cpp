//
// Created by Andrea Novellini on 30/08/2025.
//

#ifndef LINEARLY_INTERPOLATED_MAPPING_IMPL_HPP
#define LINEARLY_INTERPOLATED_MAPPING_IMPL_HPP
#include "linearly_interpolated_mapping.hpp"
#include <cmath>
#include "fast_log2.hpp"

namespace datasketches {

inline LinearlyInterpolatedMapping::LinearlyInterpolatedMapping(const double& relative_accuracy):
  LogLikeIndexMapping(compute_gamma(require_valid_relative_accuracy(relative_accuracy), CORRECTING_FACTOR), index_offset_shift(relative_accuracy)) {}

inline LinearlyInterpolatedMapping::LinearlyInterpolatedMapping(const double& gamma, const double& index_offset):
  LogLikeIndexMapping(gamma, index_offset) {}

constexpr double LinearlyInterpolatedMapping::correcting_factor() {
  return CORRECTING_FACTOR;
}

constexpr double LinearlyInterpolatedMapping::base() {
  return BASE;
}

inline double LinearlyInterpolatedMapping::log(const double& value) const {
  return fast_log2(value);
}

inline double LinearlyInterpolatedMapping::log_inverse(const double& index) const {
  return fast_log2_inverse(index);
}

inline IndexMappingLayout LinearlyInterpolatedMapping::layout() const {
  return IndexMappingLayout::LOG_LINEAR;
}

inline double LinearlyInterpolatedMapping::index_offset_shift(const double& relative_accuracy) const {
  return 1 / (std::log1p(2 * relative_accuracy / (1 - relative_accuracy)));
}
}

#endif //LINEARLY_INTERPOLATED_MAPPING_IMPL_HPP
