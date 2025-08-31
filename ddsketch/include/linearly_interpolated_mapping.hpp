//
// Created by Andrea Novellini on 30/08/2025.
//

#ifndef LINEARLY_INTERPOLATED_MAPPING_HPP
#define LINEARLY_INTERPOLATED_MAPPING_HPP

#include "index_mapping.hpp"
#include "log_like_index_mapping.hpp"
#include <numbers>


namespace datasketches {

class LinearlyInterpolatedMapping : public LogLikeIndexMapping<LinearlyInterpolatedMapping> {
public:
  explicit LinearlyInterpolatedMapping(const double& relative_accuracy);
  LinearlyInterpolatedMapping(const double& gamma, const double& index_offset);

  double log(const double& value) const override;
  double log_inverse(const double& index) const override;

  IndexMappingLayout layout() const override;

  static constexpr double BASE = 2.0;
  static constexpr double CORRECTING_FACTOR = std::numbers::log2e;

private:


  double index_offset_shift(const double& relative_accuracy) const;
};
}

#include "linearly_interpolated_mapping_impl.hpp"

#endif //LINEARLY_INTERPOLATED_MAPPING_HPP
