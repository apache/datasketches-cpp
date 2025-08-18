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
#ifndef BIN_H
#define BIN_H

#include <cstdint>
#include <string>

namespace datasketches {
class Bin {
public:
  Bin(int index, double count);
  ~Bin() = default;
  bool operator==(const Bin& other) const;
  int hashCode() const;
  std::string toString() const;
  double getCount() const;
  int getIndex() const;

private:
  int index;
  double count;
};
}

#include "bin_impl.hpp"

#endif //BIN_H
