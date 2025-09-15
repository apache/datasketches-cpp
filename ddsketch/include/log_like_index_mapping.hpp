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
#ifndef LOG_LIKE_INDEX_MAPPING_HPP
#define LOG_LIKE_INDEX_MAPPING_HPP
#include "index_mapping.hpp"
#include <cmath>
#include <memory>
#include <stdexcept>

namespace datasketches {


template<class Derived>
class LogLikeIndexMapping : public IndexMapping<Derived> {
public:
  LogLikeIndexMapping(const double& gamma, const double& index_offset);

  int index(const double& value) const;
  double value(int index) const;
  double lower_bound(int index) const;
  double upper_bound(int index) const;
  double get_relative_accuracy() const;
  double min_indexable_value() const;
  double max_indexable_value() const;
  void serialize(std::ostream& os) const;

  template<class A>
  string<A> to_string() const;

  bool operator==(const LogLikeIndexMapping<Derived>& other) const;

private:
  static double compute_relative_accuracy(const double& gamma, const double& correcting_factor);
  static double require_valid_gamma(const double& gamma);
  IndexMappingLayout layout() const;

protected:
  static double require_valid_relative_accuracy(const double& relative_accuracy);
  static double compute_gamma(const double& relative_accuracy, const double& correcting_factor);
  double log(const double& value) const;
  double log_inverse(const double& value) const;

  const double gamma;
  const double index_offset;

  const double relative_accuracy;
  const double multiplier;

  Derived& derived();
  const Derived& derived() const;
};
}

#include "log_like_index_mapping_impl.hpp"

#endif //LOG_LIKE_INDEX_MAPPING_HPP
