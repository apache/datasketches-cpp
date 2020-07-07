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

#ifndef THETA_HELPERS_HPP_
#define THETA_HELPERS_HPP_

#include <string>
#include <stdexcept>

namespace datasketches {

template<bool dummy>
class checker {
public:
  static void check_sketch_type(uint8_t actual, uint8_t expected) {
      if (actual != expected) {
        throw std::invalid_argument("Sketch type mismatch: expected " + std::to_string((int)expected) + ", actual " + std::to_string((int)actual));
      }
  }

  static void check_serial_version(uint8_t actual, uint8_t expected) {
    if (actual != expected) {
      throw std::invalid_argument("Sketch serial version mismatch: expected " + std::to_string((int)expected) + ", actual " + std::to_string((int)actual));
    }
  }

  static void check_seed_hash(uint16_t actual, uint16_t expected) {
    if (actual != expected) {
      throw std::invalid_argument("Sketch seed hash mismatch: expected " + std::to_string(expected) + ", actual " + std::to_string(actual));
    }
  }
};

} /* namespace datasketches */

#endif
