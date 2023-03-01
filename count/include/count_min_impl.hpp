#ifndef COUNT_MIN_IMPL_HPP_
#define COUNT_MIN_IMPL_HPP_

#include "MurmurHash3.h"
#include <random>

namespace datasketches {

template<typename W>
count_min_sketch<W>::count_min_sketch(uint8_t num_hashes, uint32_t num_buckets, uint64_t seed):
_num_hashes(num_hashes),
_num_buckets(num_buckets),
_sketch_array((num_hashes*num_buckets < 1<<30) ? num_hashes*num_buckets : 0, 0),
_seed(seed),
_total_weight(0){
  if(num_buckets < 3) throw std::invalid_argument("Using fewer than 3 buckets incurs relative error greater than 1.") ;

  // This check is to ensure later compatibility with a Java implementation whose maximum size can only
  // be 2^31-1.  We check only against 2^30 for simplicity.
  if(num_buckets*num_hashes >= 1<<30) {
    throw std::invalid_argument("These parameters generate a sketch that exceeds 2^30 elements."
                                "Try reducing either the number of buckets or the number of hash functions.") ;
  }


  std::default_random_engine rng(_seed);
  std::uniform_int_distribution<uint64_t> extra_hash_seeds(0, std::numeric_limits<uint64_t>::max());
  hash_seeds.reserve(num_hashes) ;

  for(uint64_t i=0 ; i < num_hashes ; ++i){
    hash_seeds.push_back(extra_hash_seeds(rng) + _seed); // Adds the global seed to all hash functions.
  }
}

template<typename W>
uint16_t count_min_sketch<W>::get_num_hashes() const{
    return _num_hashes ;
}

template<typename W>
uint32_t count_min_sketch<W>::get_num_buckets() const{
    return _num_buckets ;
}

template<typename W>
uint64_t count_min_sketch<W>::get_seed() const {
    return _seed ;
}

template<typename W>
double count_min_sketch<W>::get_relative_error() const{
  return exp(1.0) / double(_num_buckets) ;
}

template<typename W>
W count_min_sketch<W>::get_total_weight() const{
  return _total_weight ;
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
  return ceil(exp(1.0) / relative_error);
}

template<typename W>
uint8_t count_min_sketch<W>::suggest_num_hashes(double confidence){
  /*
   * Function to help users select a number of hashes for a given confidence
   * e.g. confidence = 1 - failure probability
   * failure probability == delta in the literature.
   */
  if(confidence < 0. || confidence > 1.0){
    throw std::invalid_argument( "Confidence must be between 0 and 1.0 (inclusive)." );
  }
  return std::min<uint8_t>( ceil(log(1.0/(1.0 - confidence))), UINT8_MAX) ;
}

template<typename W>
std::vector<uint64_t> count_min_sketch<W>::get_hashes(const void* item, size_t size) const{
  /*
   * Returns the hash locations for the input item using the original hashing
   * scheme from [1].
   * Generate _num_hashes separate hashes from calls to murmurmhash.
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
  std::vector<uint64_t> sketch_update_locations; //(_num_hashes) ;
  sketch_update_locations.reserve(_num_hashes) ;

  uint64_t hash_seed_index = 0 ;
  for(const auto &it : hash_seeds){
    HashState hashes;
    MurmurHash3_x64_128(item, size, it, hashes); // ? BEWARE OVERFLOW.
    uint64_t hash = hashes.h1 ;
    bucket_index = hash % _num_buckets ;
    sketch_update_locations.push_back((hash_seed_index * _num_buckets) + bucket_index) ;
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
    estimates.push_back(_sketch_array[h]) ;
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
  W magnitude = (weight >= 0) ? weight : -weight ;
  _total_weight += magnitude ;
  std::vector<uint64_t> hash_locations = get_hashes(item, size) ;
  for (auto h: hash_locations){
    _sketch_array[h] += weight ;
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
  auto it = _sketch_array.begin() ; // This is a std::vector iterator.
  auto other_it = other_sketch.begin() ; //This is a const iterator over the other sketch.
  while(it != _sketch_array.end()){
    *it += *other_it ;
    ++it ;
    ++other_it ;
  }
  _total_weight += other_sketch.get_total_weight() ;
}

// Iterators
template<typename W>
typename count_min_sketch<W>::const_iterator count_min_sketch<W>::begin() const {
  return _sketch_array.begin();
}

template<typename W>
typename count_min_sketch<W>::const_iterator count_min_sketch<W>::end() const {
return _sketch_array.end();
}

template<typename W>
void count_min_sketch<W>::serialize(std::ostream& os) const {
  const uint8_t preamble_longs = (_total_weight == 0) ? 1 : 0; // is_empty is 1 if weight == 0
  write(os, preamble_longs);

  // These are placeholders for now
  const uint8_t serial_version = 1 ;
  write(os, serial_version) ;
  const uint8_t family_id = 1 ;
  write(os, family_id) ;
  const uint8_t flags = 1 ;
  write(os, flags) ;

  // Now write 4 zeros to complete the 0th block of 8 bytes
  const uint32_t null_characters_32 = 0 ;
  write(os, null_characters_32) ;

  // At index 1 for the writing
  // Sketch parameters at bytes[1][0,1,2,3,4]
  const uint8_t nhashes = _num_hashes ;
  write(os, nhashes) ;
  const uint32_t nbuckets = _num_buckets ;
  write(os, nbuckets) ;

  // Seed hash at bytes[1][5, 6]
  const uint16_t seed_hash(compute_seed_hash(_seed));
  write(os, seed_hash);

  // Now write 1 zero to complete the bytes[1]7]
  const uint8_t null_characters_8 = 0 ;
  write(os, null_characters_8) ;

  if (preamble_longs == 1) return ; // sketch is empty, no need to write any more.

  // In the next 8 bytes, write the total weight
  const W t_weight = _total_weight ;
  write(os, t_weight) ;

  // Any remaining bytes are consumed by writing the array values.
  auto it = _sketch_array.begin() ;
  while(it != _sketch_array.end()){
    write(os, *it);
    ++it ;
  }
}

template<typename W>
count_min_sketch<W> count_min_sketch<W>::deserialize(std::istream& is, uint64_t seed) {

  const auto is_empty = read<uint8_t>(is) ;
  const auto serial_version = read<uint8_t>(is) ;
  const auto family_id = read<uint8_t>(is) ;
  const auto flags = read<uint8_t>(is) ;
  read<uint32_t>(is) ; // 4 unused bytes

  // Sketch parameters
  const auto nhashes = read<uint8_t>(is);
  const auto nbuckets = read<uint32_t>(is) ;
  const auto seed_hash = read<uint16_t>(is) ;
  read<uint8_t>(is) ; // 1 unused byte

  if (seed_hash != compute_seed_hash(seed)) {
    throw std::invalid_argument("Incompatible seed hashes: " + std::to_string(seed_hash) + ", "
                                + std::to_string(compute_seed_hash(seed)));
  }
  count_min_sketch<W> c(nhashes, nbuckets, seed) ;
  if (is_empty == 1) return c ; // sketch is empty, no need to read any more.

  // Set the sketch weight and read in the sketch values
  const auto weight = read<W>(is) ;
  c._total_weight += weight ;
  read(is, c._sketch_array.data(), sizeof(W) * c._sketch_array.size());

  return c ;
}
} /* namespace datasketches */

#endif
