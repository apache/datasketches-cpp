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

/**
 * @class Bin
 * @brief Represents a bucket of counts in a DDSketch store.
 *
 * A Bin corresponds to a mapped value index and its associated count.
 * It is the fundamental unit used in DenseStore, SparseStore, and their variants.
 */
class Bin {
public:
  /**
    * @brief Construct a new Bin.
    * @param index The index representing the mapped value bucket.
    * @param count The number of samples in this bin.
    */
  Bin(int index, double count);

  ~Bin() = default;

  /**
   * @brief Equality operator.
   * @param other The other bin to compare with.
   * @return True if both bins have the same index and count.
   */
  bool operator==(const Bin& other) const;
  std::string to_string() const;

  /**
   * @brief Get the count of this bin.
   * @return The number of samples in the bin.
   */
  double get_count() const;

  /**
   * @brief Get the index of this bin.
   * @return The integer index.
   */
  int get_index() const;

private:
  int index;
  double count;
};
}

#include "bin_impl.hpp"

#endif //BIN_H
