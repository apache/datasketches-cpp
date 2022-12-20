#ifndef COUNT_MIN_HPP_
#define COUNT_MIN_HPP_

#include <cstdint>
#include <vector>
#include <stdint.h>

#include "common_defs.hpp"

namespace datasketches {

class count_min_sketch{
public:
  uint64_t num_hashes, num_buckets, seed ;

  /**
  * Creates an instance of the sketch given parameters num_hashes, num_buckets and hash seed, `seed`.
  * @param num_hashes : number of hash functions in the sketch. Equivalently the number of rows in the array
  * @param num_buckets : number of buckets that hash functions map into. Equivalently the number of columns in the array
  * @param seed for hash function
  */
  count_min_sketch(uint64_t num_hashes, uint64_t num_buckets, uint64_t seed = DEFAULT_SEED) ;

  std::vector<uint64_t> sketch ; // the sketch array data structure

  /**
  * @return configured num_hashes of this sketch
  */
  uint64_t get_num_hashes() ;

  /**
  * @return configured num_buckets of this sketch
  */
  uint64_t get_num_buckets() ;

  /**
  * @return configured seed of this sketch
  */
  uint64_t get_seed() ;

  /**
  * @return vector of the sketch configuration: {num_hsahes, num_buckets, seed}
  */
  std::vector<uint64_t> get_config() ; // Sketch parameter configuration -- needed for merging.

  /**
  * @return vector of the sketch data structure
  */
  std::vector<uint64_t> get_sketch() ; // Sketch parameter configuration -- needed for merging.

//  /**
//  * void function sets the hash parameters a and b for the bucket assignment in each row.
//  */
//   void set_hash_parameters() ;

  /**
  * @return vector of uint64_t which each represent the index to which `value' must update in the sketch
  */
  std::vector<uint64_t> get_hashes(const void* value) ;

  /**
   * void function which inserts an item into the sketch
   */
  void update(const void* value) ;

  /**
   * @return the estimated frequency of item
   */
   uint64_t get_estimate(const void* item) ;




};

} /* namespace datasketches */

#include "count_min_impl.hpp"

#endif