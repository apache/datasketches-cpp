//
// Created by Andrea Novellini on 31/08/2025.
//

#ifndef QUADRATICALLY_INTERPOLATED_MAPPING_HPP
#define QUADRATICALLY_INTERPOLATED_MAPPING_HPP
#include "logarithmic_mapping.hpp"

namespace datasketches {

class QuadraticallyInterpolatedMapping : public LogLikeIndexMapping<QuadraticallyInterpolatedMapping> {
public:
  explicit QuadraticallyInterpolatedMapping(const double& relative_accuracy);
  QuadraticallyInterpolatedMapping(const double& gamma, const double& index_offset);
  double log(const double& value) const override;
  double log_inverse(const double& index) const override;

  IndexMappingLayout layout() const override;

  static constexpr double BASE = 2.0;
  static constexpr double CORRECTING_FACTOR = 3.0 / (4.0 * std::numbers::ln2);
  static constexpr double ONE_THIRD = 1.0 / 3.0;
};
}

#include "quadratically_interpolated_mapping_impl.hpp"

#endif //QUADRATICALLY_INTERPOLATED_MAPPING_HPP
