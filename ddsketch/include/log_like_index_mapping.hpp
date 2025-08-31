//
// Created by geonove on 8/30/25.
//

#ifndef LOG_LIKE_INDEX_MAPPING_HPP
#define LOG_LIKE_INDEX_MAPPING_HPP
#include "index_mapping.hpp"
#include <cmath>
#include <memory>
#include <stdexcept>

namespace datasketches {


template<class Derived>
class LogLikeIndexMapping : public IndexMapping {
public:
  LogLikeIndexMapping(const double& gamma, const double& index_offset);

  int index(const double& value) const override;
  double value(int index) const override;
  double lower_bound(int index) const override;
  double upper_bound(int index) const override;
  double get_relative_accuracy() const override;
  double min_indexable_value() const override;
  double max_indexable_value() const override;
  void encode(std::ostream& os) override;

  bool operator==(const LogLikeIndexMapping<Derived>& other) const;

private:
  static double compute_relative_accuracy(const double& gamma, const double& correcting_factor);
  static double require_valid_gamma(const double& gamma);
  virtual IndexMappingLayout layout() const = 0;

protected:
  static double require_valid_relative_accuracy(const double& relative_accuracy);
  static double compute_gamma(const double& relative_accuracy, const double& correcting_factor);
  virtual double log(const double& value) const = 0;
  virtual double log_inverse(const double& value) const = 0;

  const double gamma;
  const double index_offset;

  const double relative_accuracy;
  const double multiplier;
};
}

#include "log_like_index_mapping_impl.hpp"

#endif //LOG_LIKE_INDEX_MAPPING_HPP
