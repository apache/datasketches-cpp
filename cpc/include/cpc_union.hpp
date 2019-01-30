/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef CPC_UNION_HPP_
#define CPC_UNION_HPP_

#include "fm85Merging.h"
#include "cpc_sketch.hpp"

namespace datasketches {

/*
 * High performance C++ implementation of Compressed Probabilistic Counting sketch
 *
 * author Kevin Lang
 * author Alexander Saydakov
 */

UG85* ug85Copy(UG85* other) {
  UG85* copy(new UG85(*other));
  if (other->accumulator != nullptr) copy->accumulator = fm85Copy(other->accumulator);
  if (other->bitMatrix != nullptr) {
    uint32_t k = 1 << copy->lgK;
    copy->bitMatrix = (U64 *) malloc ((size_t) (k * sizeof(U64)));
    std::copy(&other->bitMatrix[0], &other->bitMatrix[k], copy->bitMatrix);
  }
  return copy;
}

class cpc_union {
  public:
    explicit cpc_union(uint8_t lg_k = CPC_DEFAULT_LG_K, uint64_t seed = DEFAULT_SEED) : seed(seed) {
      fm85Init();
      if (lg_k < CPC_MIN_LG_K or lg_k > CPC_MAX_LG_K) {
        throw std::invalid_argument("lg_k must be >= " + std::to_string(CPC_MIN_LG_K) + " and <= " + std::to_string(CPC_MAX_LG_K) + ": " + std::to_string(lg_k));
      }
      state = ug85Make(lg_k);
    }

    cpc_union(const cpc_union& other) {
      seed = other.seed;
      state = ug85Copy(other.state);
    }

    cpc_union& operator=(cpc_union other) {
      seed = other.seed;
      std::swap(state, other.state); // @suppress("Invalid arguments")
      return *this;
    }

    ~cpc_union() {
      ug85Free(state);
    }

    void update(const cpc_sketch& sketch) {
      const uint16_t seed_hash_union = compute_seed_hash(seed);
      const uint16_t seed_hash_sketch = compute_seed_hash(sketch.seed);
      if (seed_hash_union != seed_hash_sketch) {
        throw std::invalid_argument("Incompatible seed hashes: " + std::to_string(seed_hash_union) + ", "
            + std::to_string(seed_hash_sketch));
      }
      ug85MergeInto(state, sketch.state);
    }

    cpc_sketch_unique_ptr get_result() const {
      cpc_sketch_unique_ptr sketch_ptr(
          new (fm85alloc(sizeof(cpc_sketch))) cpc_sketch(ug85GetResult(state), seed),
          [](cpc_sketch* s) { s->~cpc_sketch(); fm85free(s); }
      );
      return std::move(sketch_ptr);
    }

  private:
    UG85* state;
    uint64_t seed;
};

} /* namespace datasketches */

#endif
