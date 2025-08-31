//
// Created by Andrea Novellini on 31/08/2025.
//

#ifndef LOGARITHMIC_MAPPING_HPP
#define LOGARITHMIC_MAPPING_HPP
#include <numbers>

#include "log_like_index_mapping.hpp"

namespace datasketches {

class LogarithmicMapping : public LogLikeIndexMapping<LogarithmicMapping> {
public:
  explicit LogarithmicMapping(const double& relative_accuracy);
  LogarithmicMapping(const double& gamma, const double& index_offset);

  double log(const double& value) const override;
  double log_inverse(const double& index) const override;

  IndexMappingLayout layout() const override;

  static constexpr double BASE = std::numbers::e;
  static constexpr double CORRECTING_FACTOR = 1.0;
};
}

#include "logarithmic_mapping_impl.hpp"

#endif //LOGARITHMIC_MAPPING_HPP
