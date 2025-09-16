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

#ifndef BIN_IMPL_H
#define BIN_IMPL_H

#include "bin.hpp"

namespace datasketches {
inline Bin::Bin(int index, double count): index(index), count(count) {};

inline bool Bin::operator==(const Bin& other) const {
  if (this == &other) {
    return true;
  }
  return index == other.index && count == other.count;
};

inline double Bin::get_count() const {
  return count;
}

inline int Bin::get_index() const {
  return index;
}

inline std::string Bin::to_string() const {
  return "Bin{index= " + std::to_string(index) + ", count= " + std::to_string(count) + "}";
}

}
#endif //BIN_IMPL_H
