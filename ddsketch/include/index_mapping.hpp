//
// Created by geonove on 8/30/25.
//

#ifndef INDEX_MAPPING_HPP
#define INDEX_MAPPING_HPP
#include <cstdint>
#include <iosfwd>

namespace datasketches {

enum class IndexMappingLayout : uint8_t {
  LOG,
  LOG_LINEAR,
  LOG_QUADRATIC,
  LOG_CUBIC,
  LOG_QUARTIC,
};

std::ostream& operator<<(std::ostream& os, const IndexMappingLayout& obj);

class IndexMapping {
public:
  virtual int index(const double& value) const = 0;
  virtual double value(int index) const = 0;
  virtual double lower_bound(int index) const = 0;
  virtual double upper_bound(int index) const = 0;
  virtual double get_relative_accuracy() const = 0;
  virtual double min_indexable_value() const = 0;
  virtual double max_indexable_value() const = 0;
  virtual void encode(std::ostream& os) = 0;

  template<IndexMappingLayout layout>
  static IndexMapping* decode(std::istream& is);

  virtual ~IndexMapping() = default;
};

}

#include "index_mapping_impl.hpp"
#endif //INDEX_MAPPING_HPP
