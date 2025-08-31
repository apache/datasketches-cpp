//
// Created by Andrea Novellini on 31/08/2025.
//

#ifndef LOGARITHMIC_MAPPING_IMPL_HPP
#define LOGARITHMIC_MAPPING_IMPL_HPP
#include "logarithmic_mapping.hpp"

namespace datasketches {

inline LogarithmicMapping::LogarithmicMapping(const double& relative_accuracy) :
  LogLikeIndexMapping<LogarithmicMapping>(compute_gamma(require_valid_relative_accuracy(relative_accuracy), 1.0), 0.0) {}

inline LogarithmicMapping::LogarithmicMapping(const double& gamma, const double& index_offset) :
  LogLikeIndexMapping<LogarithmicMapping>(gamma, index_offset) {}

inline double LogarithmicMapping::log(const double& value) const {
  return std::log(value);
}

inline double LogarithmicMapping::log_inverse(const double &index) const {
  return std::exp(index);
}

inline IndexMappingLayout LogarithmicMapping::layout() const {
  return IndexMappingLayout::LOG;
}
}


#endif //LOGARITHMIC_MAPPING_IMPL_HPP
