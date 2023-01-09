#ifndef COUNT_MIN_HPP_
#define COUNT_MIN_HPP_

#include <cstdint>
#include <vector>
//#include <stdint.h>
#include <iterator>
#include <algorithm>

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
  static_assert(std::is_arithmetic<T>::value, "Arithmetic type expected");
public:
  uint64_t num_hashes, num_buckets, seed, sketch_length ;
  T total_weight = 0;
  std::vector<uint64_t> hash_seeds ;

  /**
  * Creates an instance of the sketch given parameters num_hashes, num_buckets and hash seed, `seed`.
  * @param num_hashes : number of hash functions in the sketch. Equivalently the number of rows in the array
  * @param num_buckets : number of buckets that hash functions map into. Equivalently the number of columns in the array
  * @param seed for hash function
  *
  * The template type T is the type of the vector that contains the weights, not the objects inserted into the sketch.
   * The items inserted into the sketch can be arbitrary type, so long as they are hashable via murmurhash.
   * Only update and estimate methods are added for uint64_t and string types.
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
   * @return epsilon : double
   * The maximum permissible error for any frequency estimate query.
   * epsilon = e / num_buckets
   */
   double get_relative_error() ;

  /**
  * @return total_weight : typename T
  * The total weight currently inserted into the stream.
  */
  T get_total_weight() ;

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

  /**
  * @return vector of uint64_t which each represent the index to which `value' must update in the sketch
  */
  std::vector<uint64_t> get_hashes(const void* item, size_t size) ;

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
   * @return f_est : type T -- the estimated frequency of item
   * Guarantee satisfies that f_est
   * f_true - relative_error*total_weight <= f_est <= f_true
   */
   T get_estimate(const void* item, size_t size) ;

   /*
    * @return the upper bound on the true frequency of the item
    * f_true <= f_est + relative_error*total_weight
    */
   T get_upper_bound(const void* item, size_t size) ;
   T get_upper_bound(uint64_t) ;
   T get_upper_bound(const std::string& item) ;

   /*
   * @return the upper bound on the true frequency of the item
   * f_true <= f_est + relative_error*total_weight
   */
  T get_lower_bound(const void* item, size_t size) ;
  T get_lower_bound(uint64_t) ;
  T get_lower_bound(const std::string& item) ;

  /**
  * void function for generic updates.
  */
  void update(const void* item, size_t size, T weight) ;

  /**
  * void function which inserts an item of type uint64_t into the sketch
  */
  void update(uint64_t item, T weight) ;
  void update(uint64_t item) ;

  /**
   * void function which inserts an item of type std::string into the sketch
   */
  void update(const std::string& item, T weight) ;
  void update(const std::string& item) ;

  /*
  * merges a separate count_min_sketch into this count_min_sketch.
  */
  void merge(count_min_sketch<T> &other_sketch) ;

};


} /* namespace datasketches */

#include "count_min_impl.hpp"

#endif