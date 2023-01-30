#ifndef COUNT_MIN_HPP_
#define COUNT_MIN_HPP_

#include <iterator>
#include "common_defs.hpp"

namespace datasketches {

  /*
   * C++ implementation of the CountMin sketch data structure of Cormode and Muthukrishnan.
   * [1] - http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf
   * The template type W is the type of the vector that contains the weights of the objects inserted into the sketch,
   * not the type of the input items themselves.
   * @author Charlie Dickens
   */

template<typename W>
class count_min_sketch{
  static_assert(std::is_arithmetic<W>::value, "Arithmetic type expected");
public:

  /**
  * Creates an instance of the sketch given parameters num_hashes, num_buckets and hash seed, `seed`.
  * @param num_hashes : number of hash functions in the sketch. Equivalently the number of rows in the array
  * @param num_buckets : number of buckets that hash functions map into. Equivalently the number of columns in the array
  * @param seed for hash function
  *
  * The items inserted into the sketch can be arbitrary type, so long as they are hashable via murmurhash.
  * Only update and estimate methods are added for uint64_t and string types.
  */
  count_min_sketch(uint8_t num_hashes, uint32_t num_buckets, uint64_t seed = DEFAULT_SEED) ;

  /**
  * @return configured num_hashes of this sketch
  */
  uint16_t get_num_hashes() const;

  /**
  * @return configured num_buckets of this sketch
  */
  uint32_t get_num_buckets() const;

  /**
  * @return configured seed of this sketch
  */
  uint64_t get_seed()  const;

  /**
   * @return epsilon : double
   * The maximum permissible error for any frequency estimate query.
   * epsilon = ceil(e / num_buckets)
   */
   double get_relative_error() const;

  /**
  * @return total_weight : typename W
  * The total weight currently inserted into the stream.
  */
  W get_total_weight() const;

  /*
 * @param relative_error : double -- the desired accuracy within which estimates should lie.
 * For example, when relative_error = 0.05, then the returned frequency estimates satisfy the
 * `relative_error` guarantee that never overestimates the weights but may underestimate the weights
 * by 5% of the total weight in the sketch.
 * @return number_of_buckets : the number of hash buckets at every level of the
 * sketch required in order to obtain the specified relative error.
 * [1] - Section 3 ``Data Structure'', page 6.
 */
  static uint32_t suggest_num_buckets(double relative_error) ;

  /*
   * @param confidence : double -- the desired confidence with which estimates should be correct.
   * For example, with 95% confidence, frequency estimates satisfy the `relative_error` guarantee.
   * @return number_of_hashes : the number of hash functions that are required in
   * order to achieve the specified confidence of the sketch.
   * confidence = 1 - delta, with delta denoting the sketch failure probability in the literature.
   * [1] - Section 3 ``Data Structure'', page 6.
  */
  static uint8_t suggest_num_hashes(double confidence) ;

  /**
   * Specific get_estimate function for uint64_t type
   * see generic get_estimate function
   * @param item : uint64_t type.
   * @return an estimate of the item's frequency.
   */
  W get_estimate(uint64_t item) const ;

   /**
   * Specific get_estimate function for std::string type
   * see generic get_estimate function
   * @param item : std::string type
   * @return an estimate of the item's frequency.
   */
  W get_estimate(const std::string& item) const;

  /**
  * This is the generic estimate query function for any of the given datatypes.
  * Query the sketch for the estimate of a given item.
  * @param item : pointer to the data item to be query from the sketch.
  * @param size : size_t
  * @return the estimated frequency of the item denoted f_est satisfying
  * f_true - relative_error*total_weight <= f_est <= f_true
  */
   W get_estimate(const void* item, size_t size) const ;

  /**
  * Query the sketch for the upper bound of a given item.
  * @param item : uint64_t or std::string to query
  * @return the upper bound on the true frequency of the item
  * f_true <= f_est + relative_error*total_weight
  */
   W get_upper_bound(const void* item, size_t size) const;
   W get_upper_bound(uint64_t) const ;
   W get_upper_bound(const std::string& item) const;

  /**
  * Query the sketch for the lower bound of a given item.
  * @param item : uint64_t or std::string to query
  * @return the lower bound for the query result, f_est, on the true frequency, f_est of the item
  * f_true - relative_error*total_weight <= f_est
  */
  W get_lower_bound(const void* item, size_t size) const ;
  W get_lower_bound(uint64_t) const ;
  W get_lower_bound(const std::string& item) const ;

  /*
  * Update this sketch with given data of any type.
  * This is a "universal" update that covers all cases above,
  * but may produce different hashes.
  * @param item pointer to the data item to be inserted into the sketch.
  * @param size of the data in bytes
  * @return vector of uint64_t which each represent the index to which `value' must update in the sketch
  */
  void update(const void* item, size_t size, W weight) ;

  /**
  * Update this sketch with a given uint64_t item.
  * @param item : uint64_t to update the sketch with
  * @param weight : arithmetic type
  *  void function which inserts an item of type uint64_t into the sketch
  */
  void update(uint64_t item, W weight) ;
  void update(uint64_t item) ;

  /**
   * Update this sketch with a given string.
   * @param item : string to update the sketch with
   * @param weight : arithmetic type
   * void function which inserts an item of type std::string into the sketch
   */
  void update(const std::string& item, W weight) ;
  void update(const std::string& item) ;

  /*
  * merges a separate count_min_sketch into this count_min_sketch.
  */
  void merge(const count_min_sketch<W> &other_sketch) ;

  // Iterators
  using const_iterator = typename std::vector<W>::const_iterator ;
  const_iterator begin() const;
  const_iterator end() const;

private:
  uint16_t num_hashes ;
  uint32_t num_buckets ;
  uint64_t seed ;
  W total_weight ;
  std::vector<uint64_t> hash_seeds ;
  std::vector<W> sketch_array ; // the array stored by the sketch


  /*
   * Obtain the hash values when inserting an item into the sketch.
   * @param item pointer to the data item to be inserted into the sketch.
   * @param size of the data in bytes
   * @return vector of uint64_t which each represent the index to which `value' must update in the sketch
   */
  std::vector<uint64_t> get_hashes(const void* item, size_t size) const;

};

} /* namespace datasketches */

#include "count_min_impl.hpp"

#endif
