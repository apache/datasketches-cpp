#ifndef COUNT_MIN_HPP_
#define COUNT_MIN_HPP_

#include <cstdint>
#include <vector>
#include <stdint.h>

#include "common_defs.hpp"

namespace datasketches {

  /*
   * C++ implementation of the CountMin sketch data structure of Cormode and Muthukrishnan.
   * [1] - http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf
   * @author Charlie Dickens
   */

template<typename T> class count_min_sketch ;

template<typename T>
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

  //std::vector<uint64_t> sketch ; // the sketch array data structure
  std::vector<T> sketch ; // the sketch array data structure

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
  std::vector<T> get_sketch() ; // Sketch parameter configuration -- needed for merging.

  /*
 * @return number_of_buckets : the number of hash buckets at every level of the
 * sketch required in order to obtain the specified relative error.
 * [1] - Section 3 ``Data Structure'', page 6.
 */
  static uint64_t suggest_num_buckets(double relative_error) ;

  /*
  * @return number_of_hashes : the number of hash functions that are required in
   * order to achieve the specified confidence of the sketch.
   * confidence = 1 - delta, with delta denoting the sketch failure probability in the literature.
   * [1] - Section 3 ``Data Structure'', page 6.
  */
  static uint64_t suggest_num_hashes(double confidence) ;



//  /**
//  * void function sets the hash parameters a and b for the bucket assignment in each row.
//  */
//   void set_hash_parameters() ;

  /**
  * @return vector of uint64_t which each represent the index to which `value' must update in the sketch
  */
  std::vector<uint64_t> get_hashes(const void* value, size_t size) ;

  /**
   * @param item : uint64_t type
   * @return an estimate of the item's frequency.
   */
  T get_estimate(uint64_t item) ;

   /**
   * @param item : std::string type
   * @return an estimate of the item's frequency.
   */
  T get_estimate(const std::string& item) ;

  /**
   * @return the estimated frequency of item
   */
   T get_estimate(const void* item, size_t size) ;

  /**
  * void function which inserts an item of type uint64_t into the sketch
  */
  void update(uint64_t item) ;

  /**
   * void function which inserts an item of type std::string into the sketch
   */
  void update(const std::string& item) ;

  /**
  * void function for generic updates.
  */
  void update(const void* item, size_t size) ;






};

} /* namespace datasketches */

#include "count_min_impl.hpp"

#endif