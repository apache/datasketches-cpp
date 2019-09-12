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

#ifndef CPC_SKETCH_HPP_
#define CPC_SKETCH_HPP_

#include <iostream>
#include <functional>
#include <string>

#if defined(_MSC_VER)
#include <iso646.h> // for and/or keywords
#endif // _MSC_VER

#include "fm85.h"
#include "fm85Compression.h"
#include "iconEstimator.h"
#include "fm85Confidence.h"
#include "fm85Util.h"
#include "MurmurHash3.h"
#include "cpc_common.hpp"

namespace datasketches {

/*
 * High performance C++ implementation of Compressed Probabilistic Counting sketch
 *
 * author Kevin Lang
 * author Alexander Saydakov
 */

typedef std::unique_ptr<void, std::function<void(void*)>> ptr_with_deleter;

// forward-declarations
template<typename A> class cpc_sketch_alloc;
template<typename A> class cpc_union_alloc;

// allocation and initialization of global compression tables
// call this before anything else if you want to control the initialization time
// or you want to use a custom memory allocation and deallocation mechanism
// otherwise initialization happens during instantiation of the first cpc_sketch or cpc_union
// it is safe to call more than once assuming no race conditions
// this is not thread safe! neither is the rest of the library
void cpc_init(void* (*alloc)(size_t) = &malloc, void (*dealloc)(void*) = &free);

// optional deallocation of globally allocated compression tables
void cpc_cleanup();

template<typename A>
class cpc_sketch_alloc {
  public:

    explicit cpc_sketch_alloc(uint8_t lg_k = CPC_DEFAULT_LG_K, uint64_t seed = DEFAULT_SEED);
    cpc_sketch_alloc(const cpc_sketch_alloc<A>& other);
    cpc_sketch_alloc<A>& operator=(cpc_sketch_alloc<A> other);
    ~cpc_sketch_alloc();

    bool is_empty() const;
    double get_estimate() const;
    double get_lower_bound(unsigned kappa) const;
    double get_upper_bound(unsigned kappa) const;

    void update(const std::string& value);
    void update(uint64_t value);
    void update(int64_t value);

    // for compatibility with Java implementation
    void update(uint32_t value);
    void update(int32_t value);
    void update(uint16_t value);
    void update(int16_t value);
    void update(uint8_t value);
    void update(int8_t value);
    void update(double value);
    void update(float value);

    // This is a "universal" update that covers all cases above, but may produce different hashes
    // Be very careful to hash input values consistently using the same approach over time,
    // on different platforms and while passing sketches from or to Java environment
    // Otherwise two sketches that should represent overlapping sets will be disjoint
    // For instance, for signed 32-bit values call update(int32_t) method above,
    // which does widening conversion to int64_t, if compatibility with Java is expected
    void update(const void* value, int size);

    // prints a sketch summary to a given stream
    void to_stream(std::ostream& os) const;

    void serialize(std::ostream& os) const;
    std::pair<ptr_with_deleter, const size_t> serialize(unsigned header_size_bytes = 0) const;

    static cpc_sketch_alloc<A> deserialize(std::istream& is, uint64_t seed = DEFAULT_SEED);
    static cpc_sketch_alloc<A> deserialize(const void* bytes, size_t size, uint64_t seed = DEFAULT_SEED);

    // for debugging
    uint64_t get_num_coupons() const;

    // for debugging
    // this should catch some forms of corruption during serialization-deserialization
    bool validate() const;

    friend cpc_union_alloc<A>;

  private:
    static const uint8_t SERIAL_VERSION = 1;
    static const uint8_t FAMILY = 16;

    enum flags { IS_BIG_ENDIAN, IS_COMPRESSED, HAS_HIP, HAS_TABLE, HAS_WINDOW };

    FM85* state;
    uint64_t seed;

    // for deserialization and cpc_union::get_result()
    cpc_sketch_alloc(FM85* state, uint64_t seed = DEFAULT_SEED);

    static uint8_t get_preamble_ints(const FM85* state);
    static inline void write_hip(const FM85* state, std::ostream& os);
    static inline void read_hip(FM85* state, std::istream& is);
    static inline size_t copy_hip_to_mem(const FM85* state, void* dst);
    static inline size_t copy_hip_from_mem(FM85* state, const void* src);
    static inline size_t copy_to_mem(void* dst, const void* src, size_t size);
    static inline size_t copy_from_mem(const void* src, void* dst, size_t size);
};

// alias with default allocator for convenience
typedef cpc_sketch_alloc<std::allocator<void>> cpc_sketch;

} /* namespace datasketches */

#include "cpc_sketch_impl.hpp"

#endif
