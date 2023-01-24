#ifndef COUNT_MIN_IMPL_HPP_
#define COUNT_MIN_IMPL_HPP_

#include "MurmurHash3.h"
#include <random>

namespace datasketches {

template<typename W>
count_min_sketch<W>::count_min_sketch(uint16_t num_hashes, uint32_t num_buckets, uint64_t seed):
num_hashes(num_hashes),
num_buckets(num_buckets),
seed(seed){
  total_weight = 0 ;
  sketch_array.resize(num_hashes*num_buckets) ;
  if(num_buckets < 3) throw std::invalid_argument("Using fewer than 3 buckets incurs relative error greater than 1.") ;


  std::default_random_engine rng(seed);
  std::uniform_int_distribution<uint64_t> extra_hash_seeds(0, std::numeric_limits<uint64_t>::max());
  hash_seeds.reserve(num_hashes) ;

  for(uint64_t i=0 ; i < num_hashes ; ++i){
    hash_seeds.push_back(extra_hash_seeds(rng) + seed); // Adds the global seed to all hash functions.
  }
};

template<typename W>
uint16_t count_min_sketch<W>::get_num_hashes() const{
    return num_hashes ;
}

template<typename W>
uint32_t count_min_sketch<W>::get_num_buckets() const{
    return num_buckets ;
}

template<typename W>
uint64_t count_min_sketch<W>::get_seed() const {
    return seed ;
}

template<typename W>
double count_min_sketch<W>::get_relative_error() const{
  return exp(1.0) / double(num_buckets) ;
}

template<typename W>
W count_min_sketch<W>::get_total_weight() const{
  return total_weight ;
}

template<typename W>
uint32_t count_min_sketch<W>::suggest_num_buckets(double relative_error){
  /*
   * Function to help users select a number of buckets for a given error.
   * TODO: Change this when we use only power of 2 buckets.
   *
   */
  if(relative_error < 0.){
    throw std::invalid_argument( "Relative error must be at least 0." );
  }
  return ceil(exp(1.0) / relative_error) ;
}

template<typename W>
uint16_t count_min_sketch<W>::suggest_num_hashes(double confidence){
  /*
   * Function to help users select a number of hashes for a given confidence
   * e.g. confidence = 1 - failure probability
   * failure probability == delta in the literature.
   */
  if(confidence < 0. || confidence > 1.0){
    throw std::invalid_argument( "Confidence must be between 0 and 1.0 (inclusive)." );
  }
  return ceil(log(1.0/(1.0 - confidence))) ;
}

template<typename W>
std::vector<uint64_t> count_min_sketch<W>::get_hashes(const void* item, size_t size) const{
  /*
   * Returns the hash locations for the input item using the original hashing
   * scheme from [1].
   * Generate num_hashes separate hashes from calls to murmurmhash.
   * This could be optimized by keeping both of the 64bit parts of the hash
   * function, rather than generating a new one for every level.
   *
   *
   * Postscript.
   * Note that a tradeoff can be achieved over the update time and space
   * complexity of the sketch by using a combinatorial hashing scheme from
   * https://github.com/Claudenw/BloomFilter/wiki/Bloom-Filters----An-overview
   * https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf
   */
  uint64_t bucket_index ;
  std::vector<uint64_t> sketch_update_locations; //(num_hashes) ;
  sketch_update_locations.reserve(num_hashes) ;

  uint64_t hash_seed_index = 0 ;
  for(const auto &it : hash_seeds){
    HashState hashes;
    MurmurHash3_x64_128(item, size, it, hashes); // ? BEWARE OVERFLOW.
    uint64_t hash = hashes.h1 ;
    bucket_index = hash % num_buckets ;
    sketch_update_locations.push_back( (hash_seed_index * num_buckets) + bucket_index) ;
    hash_seed_index += 1 ;
  }
  return sketch_update_locations ;
}

template<typename W>
W count_min_sketch<W>::get_estimate(uint64_t item) const {return get_estimate(&item, sizeof(item));}

template<typename W>
W count_min_sketch<W>::get_estimate(const std::string& item) const {
  if (item.empty()) return 0 ; // Empty strings are not inserted into the sketch.
  return get_estimate(item.c_str(), item.length());
}

template<typename W>
W count_min_sketch<W>::get_estimate(const void* item, size_t size) const {
  /*
   * Returns the estimated frequency of the item
   */
  std::vector<uint64_t> hash_locations = get_hashes(item, size) ;
  std::vector<W> estimates ;
  for (auto h: hash_locations){
    estimates.push_back(sketch_array[h]) ;
  }
  W result = *std::min_element(estimates.begin(), estimates.end());
  return result ;
}

template<typename W>
void count_min_sketch<W>::update(uint64_t item, W weight) {
  update(&item, sizeof(item), weight);
}

template<typename W>
void count_min_sketch<W>::update(uint64_t item) {
  update(&item, sizeof(item), 1);
}

template<typename W>
void count_min_sketch<W>::update(const std::string& item, W weight) {
  if (item.empty()) return;
  update(item.c_str(), item.length(), weight);
}

template<typename W>
void count_min_sketch<W>::update(const std::string& item) {
  if (item.empty()) return;
  update(item.c_str(), item.length(), 1);
}

template<typename W>
void count_min_sketch<W>::update(const void* item, size_t size, W weight) {
  /*
   * Gets the item's hash locations and then increments the sketch in those
   * locations by the weight.
   */
  total_weight += weight ;
  std::vector<uint64_t> hash_locations = get_hashes(item, size) ;
  for (auto h: hash_locations){
    sketch_array[h] += weight ;
  }
}

template<typename W>
W count_min_sketch<W>::get_upper_bound(uint64_t item) const {return get_upper_bound(&item, sizeof(item));}

template<typename W>
W count_min_sketch<W>::get_upper_bound(const std::string& item) const {
  if (item.empty()) return 0 ; // Empty strings are not inserted into the sketch.
  return get_upper_bound(item.c_str(), item.length());
}

template<typename W>
W count_min_sketch<W>::get_upper_bound(const void* item, size_t size) const {
  return get_estimate(item, size) + get_relative_error()*get_total_weight() ;
}


template<typename W>
W count_min_sketch<W>::get_lower_bound(uint64_t item) const {return get_lower_bound(&item, sizeof(item));}

template<typename W>
W count_min_sketch<W>::get_lower_bound(const std::string& item) const {
  if (item.empty()) return 0 ; // Empty strings are not inserted into the sketch.
  return get_lower_bound(item.c_str(), item.length());
}

template<typename W>
W count_min_sketch<W>::get_lower_bound(const void* item, size_t size) const {
  return get_estimate(item, size) ;
}

template<typename W>
void count_min_sketch<W>::merge(const count_min_sketch<W> &other_sketch){
  /*
  * Merges this sketch into other_sketch sketch by elementwise summing of buckets
  */
  if(this == &other_sketch){
    throw std::invalid_argument( "Cannot merge a sketch with itself." );
  }

  bool acceptable_config =
    (get_num_hashes() == other_sketch.get_num_hashes())   &&
    (get_num_buckets() == other_sketch.get_num_buckets()) &&
    (get_seed() == other_sketch.get_seed()) ;
  if(!acceptable_config){
    throw std::invalid_argument( "Incompatible sketch configuration." );
  }

  // Merge step - iterate over the other vector and add the weights to this sketch
  auto it = sketch_array.begin() ; // This is a std::vector iterator.
  auto other_it = other_sketch.begin() ; //This is a const iterator over the other sketch.
  while(it != sketch_array.end()){
    *it += *other_it ;
    ++it ;
    ++other_it ;
  }
  total_weight += other_sketch.get_total_weight() ;
}

// Iterators
template<typename W>
typename count_min_sketch<W>::const_iterator count_min_sketch<W>::begin() const {
  return sketch_array.begin();
}

template<typename W>
typename count_min_sketch<W>::const_iterator count_min_sketch<W>::end() const {
return sketch_array.end();
}

} /* namespace datasketches */

#endif
