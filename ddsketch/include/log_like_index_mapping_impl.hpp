//
// Created by Andrea Novellini on 30/08/2025.
//

#ifndef LOG_LIKE_INDEX_MAPPING_IMPL_HPP
#define LOG_LIKE_INDEX_MAPPING_IMPL_HPP
#include "log_like_index_mapping.hpp"
#include <cmath>
#include <iomanip>

namespace datasketches {

template<class Derived>
LogLikeIndexMapping<Derived>::LogLikeIndexMapping(const double& gamma, const double& index_offset):
  gamma(require_valid_gamma(gamma)),
  index_offset(index_offset),
  relative_accuracy(compute_relative_accuracy(gamma, Derived::correcting_factor())),
  multiplier(std::log(Derived::base()) / std::log1p(gamma - 1)) {}

template<class Derived>
double LogLikeIndexMapping<Derived>::compute_relative_accuracy(const double& gamma, const double& correcting_factor) {
  const double exact_log_gamma = std::pow(gamma, correcting_factor);
  return (exact_log_gamma - 1) / (exact_log_gamma + 1);
}

template<class Derived>
double LogLikeIndexMapping<Derived>::compute_gamma(const double& relative_accuracy, const double& correcting_factor) {
  const double exact_log_gamma = (1.0 + relative_accuracy) / (1.0 - relative_accuracy);
  return std::pow(exact_log_gamma, 1.0 / correcting_factor);
}

template<class Derived>
double LogLikeIndexMapping<Derived>::require_valid_relative_accuracy(const double& relative_accuracy) {
  if (relative_accuracy <= 0 || relative_accuracy >= 1) {
    throw std::invalid_argument("relative_accuracy must be between 0 and 1");
  }
  return relative_accuracy;
}

template<class Derived>
double LogLikeIndexMapping<Derived>::require_valid_gamma(const double& gamma) {
  if (gamma <= 1) {
    throw std::invalid_argument("gamma must be greater than 1");
  }
  return gamma;
}

template<class Derived>
int LogLikeIndexMapping<Derived>::index(const double& value) const{
  assert(std::isfinite(value) && value > 0.0);
  const double index = log(value) * multiplier + index_offset;
  return static_cast<int>(std::floor(index));
}

template<class Derived>
double LogLikeIndexMapping<Derived>::value(int index) const {
  return lower_bound(index) * (1 + relative_accuracy);
}

template<class Derived>
double LogLikeIndexMapping<Derived>::lower_bound(int index) const {
  return log_inverse((index - index_offset) / multiplier);
}

template<class Derived>
double LogLikeIndexMapping<Derived>::upper_bound(int index) const {
  return lower_bound(index + 1);
}

template<class Derived>
double LogLikeIndexMapping<Derived>::get_relative_accuracy() const {
  return relative_accuracy;
}

template<class Derived>
double LogLikeIndexMapping<Derived>::min_indexable_value() const {
  const double& a = std::pow(Derived::base(), (static_cast<double>(std::numeric_limits<int>::min()) - index_offset) / multiplier + 1);
  const double& b = std::numeric_limits<double>::min() * (1 + relative_accuracy) / (1 - relative_accuracy);
  return std::max(a, b);
}

template<class Derived>
double LogLikeIndexMapping<Derived>::max_indexable_value() const {
  const double& a = std::pow(Derived::base(), (static_cast<double>(std::numeric_limits<int>::max()) - index_offset) / multiplier - 1);
  const double& b = std::numeric_limits<double>::max() / (1 + relative_accuracy);
  return std::min(a, b);
}

template<class Derived>
void LogLikeIndexMapping<Derived>::encode(std::ostream &os) {
  // TODO implement
}

}

#endif //LOG_LIKE_INDEX_MAPPING_IMPL_HPP
