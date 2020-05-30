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

#ifndef CONDITIONAL_BACK_INSERTER_HPP_
#define CONDITIONAL_BACK_INSERTER_HPP_

#include <iterator>
#include <functional>

namespace datasketches {

template <typename Container, typename Predicate>
class conditional_back_insert_iterator: public std::back_insert_iterator<Container> {
public:
  conditional_back_insert_iterator(Container& c, Predicate& p): std::back_insert_iterator<Container>(c), p(p) {}
  conditional_back_insert_iterator(Container& c, Predicate&& p): std::back_insert_iterator<Container>(c), p(std::forward<Predicate>(p)) {}

  conditional_back_insert_iterator& operator=(typename Container::const_reference value) {
    if (p(value)) std::back_insert_iterator<Container>::operator=(value);
    return *this;
  }

  conditional_back_insert_iterator& operator*() { return *this; }
  conditional_back_insert_iterator& operator++() { return *this; }
  conditional_back_insert_iterator& operator++(int) { return *this; }

private:
  Predicate p;
};

template<typename Container, typename Predicate>
conditional_back_insert_iterator<Container, Predicate> conditional_back_inserter(Container& c, Predicate& p) {
  return conditional_back_insert_iterator<Container, Predicate>(c, p);
}

template<typename Container, typename Predicate>
conditional_back_insert_iterator<Container, Predicate> conditional_back_inserter(Container& c, Predicate&& p) {
  return conditional_back_insert_iterator<Container, Predicate>(c, std::forward<Predicate>(p));
}

} /* namespace datasketches */

#endif
