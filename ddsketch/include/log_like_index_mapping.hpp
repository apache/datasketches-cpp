//
// Created by geonove on 8/30/25.
//

#ifndef LOG_LIKE_INDEX_MAPPING_HPP
#define LOG_LIKE_INDEX_MAPPING_HPP
#include "index_mapping.hpp"
#include <cmath>

namespace datasketches {

template<typename Derived, typename Layout>
class LogLikeIndexMapping : public IndexMapping<Derived, Layout> {
public:
  LogLikeIndexMapping(const double& gamma, const double& index_offset):
    gamma(gamma),
    index_offset(index_offset),
    relative_accuracy(),
    multiplier(std::log(base()) / std::log1p(gamma - 1)) {}


private:
  virtual double base() = 0;
  static double

  const double gamma;
  const double index_offset;

  const double relative_accuracy;
  const double multiplier;
};
}

#endif //LOG_LIKE_INDEX_MAPPING_HPP
