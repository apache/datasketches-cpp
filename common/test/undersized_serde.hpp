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

#ifndef UNDERSIZED_SERDE_HPP_
#define UNDERSIZED_SERDE_HPP_

#include <string>

#include "serde.hpp"

namespace datasketches {

// Reports each string as one byte shorter than it serializes it, so a sketch that serializes to bytes
// allocates too little. The capacity check of the serde must then throw std::out_of_range,
// which it can only do if the sketch passes it the true end of the buffer.
struct undersized_string_serde: serde<std::string> {
  size_t size_of_item(const std::string& item) const {
    return serde<std::string>::size_of_item(item) - 1;
  }
};

} /* namespace datasketches */

#endif
