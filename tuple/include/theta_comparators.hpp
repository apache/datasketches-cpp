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

#ifndef THETA_COMPARATORS_HPP_
#define THETA_COMPARATORS_HPP_

namespace datasketches {

template<typename Entry, typename ExtractKey>
struct compare_by_key {
  bool operator()(const Entry& a, const Entry& b) const {
    return ExtractKey()(a) < ExtractKey()(b);
  }
};

// less than

template<typename T>
class less_than {
public:
  explicit less_than(const T& value): value(value) {}
  bool operator()(const T& value) const { return value < this->value; }
private:
  T value;
};

template<typename Key, typename Entry, typename ExtractKey>
class key_less_than {
public:
  explicit key_less_than(const Key& key): key(key) {}
  bool operator()(const Entry& entry) const {
    return ExtractKey()(entry) < this->key;
  }
private:
  Key key;
};

} /* namespace datasketches */

#endif
