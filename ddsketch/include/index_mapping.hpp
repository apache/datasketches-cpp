/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
*/

#ifndef INDEX_MAPPING_HPP
#define INDEX_MAPPING_HPP
#include <cstdint>

namespace datasketches {

enum class IndexMappingLayout : uint8_t {
  LOG,
  LOG_LINEAR,
  LOG_QUADRATIC,
  LOG_CUBIC,
  LOG_QUARTIC,
};

std::ostream& operator<<(std::ostream& os, const IndexMappingLayout& obj);

template<class Derived>
class IndexMapping {
public:
  int index(const double& value) const;
  double value(int index) const;
  double lower_bound(int index) const;
  double upper_bound(int index) const;
  double get_relative_accuracy() const;
  double min_indexable_value() const;
  double max_indexable_value() const;
  void serialize(std::ostream& os) const;
  static Derived deserialize(std::istream& is);

  ~IndexMapping() = default;

protected:
  Derived& derived();
  const Derived& derived() const;
};

}

#include "index_mapping_impl.hpp"
#endif //INDEX_MAPPING_HPP
