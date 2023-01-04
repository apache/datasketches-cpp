#ifndef COUNT_MIN_IMPL_HPP_
#define COUNT_MIN_IMPL_HPP_

#include "MurmurHash3.h"

namespace datasketches {

template<typename T>
count_min_sketch<T>::count_min_sketch(uint64_t num_hashes, uint64_t num_buckets, uint64_t seed):
num_hashes(num_hashes),
num_buckets(num_buckets),
seed(seed){
  sketch.resize(num_hashes*num_buckets) ;
  //std::cout << "Vector size: " << table.capacity() <<std::endl;
  //sketch[3] = 1 ;
//  for(uint64_t i=0; i<num_hashes*num_buckets; ++i){std::cout << sketch[i] ;}
//  std::cout << std::endl;
};

template<typename T>
uint64_t count_min_sketch<T>::get_num_hashes() {
    return num_hashes ;
}

template<typename T>
uint64_t count_min_sketch<T>::get_num_buckets() {
    return num_buckets ;
}

template<typename T>
uint64_t count_min_sketch<T>::get_seed() {
    return seed ;
}

template<typename T>
std::vector<T> count_min_sketch<T>::get_sketch(){
  return sketch ;
}

template<typename T>
std::vector<uint64_t> count_min_sketch<T>::get_config(){
    std::vector<uint64_t> config(3) ;
    config = {get_num_hashes(), get_num_buckets(), get_seed()} ;
    return config ;
} ;
//
//std::vector<uint64_t> count_min_sketch::get_hashes(const void* item, size_t size){
//  /*
//   * Returns the hash locations for the input item.
//   * Approach taken from
//   * https://github.com/Claudenw/BloomFilter/wiki/Bloom-Filters----An-overview
//   */
//  uint64_t bucket_index ;
//  std::vector<uint64_t> sketch_update_locations(num_hashes) ;
//  //sketch_update_locations.resize(num_hashes) ;
//  HashState hashes;
//  std::cout<< "Hash seed: " << seed << std::endl;
//  MurmurHash3_x64_128(item, size, seed, hashes); //
//
//
//  uint64_t hash = hashes.h1 ;
//  for(uint64_t hash_idx=0; hash_idx<num_hashes; ++hash_idx){
//    hash += (hash_idx * hashes.h2) ;
//    bucket_index = hash % num_buckets ;
//    std::cout << "HASH " << hash << std::endl ;
//    sketch_update_locations.at(hash_idx) = (hash_idx * num_hashes) + bucket_index ;
//  }
//  return sketch_update_locations ;
//}
//
//void count_min_sketch::update(uint64_t item) {
//  std::cout << "Inserting " << item << std::endl;
//  update(&item, sizeof(item));
//}
//
//void count_min_sketch::update(const std::string& item) {
//  std::cout << "Inserting " << item << std::endl;
//  if (item.empty()) return;
//  update(item.c_str(), item.length());
//}
//
//void count_min_sketch::update(const void* item, size_t size) {
//  /*
//   * Gets the value's hash locations and then increments the sketch in those
//   * locations.
//   */
//  std::vector<uint64_t> hash_locations = get_hashes(item, size) ;
//  for (auto h: hash_locations){
//    sketch[h] += 1 ;
//  }
//}
//
//uint64_t count_min_sketch::get_estimate(uint64_t item) {return get_estimate(&item, sizeof(item));}
//
//uint64_t count_min_sketch::get_estimate(const std::string& item) {
//  if (item.empty()) return 0 ; // Empty strings are not inserted into the sketch.
//  return get_estimate(item.c_str(), item.length());
//}
//
//uint64_t count_min_sketch::get_estimate(const void* item, size_t size){
//  /*
//   * Returns the estimated frequency of the item
//   */
//  std::vector<uint64_t> hash_locations = get_hashes(item, size) ;
//  std::vector<uint64_t> estimates ;
//  for (auto h: hash_locations){
//    std::cout<< h << " " << sketch[h] << std::endl;
//    estimates.push_back(sketch[h]) ;
//  }
//  uint64_t result = *std::min_element(estimates.begin(), estimates.end());
//  return result ;
//}
//
//uint64_t count_min_sketch::suggest_num_buckets(double relative_error){
//  /*
//   * Function to help users select a number of buckets for a given error.
//   * TODO: Change this when we use only power of 2 buckets.
//   */
//  //std::cout<< count_min_sketch::num_buckets << std::endl;
//  if(relative_error < 0.){
//    throw std::invalid_argument( "Relative error must be at least 0." );
//  }
//  return ceil(exp(1.0) / relative_error) ;
//}
//
//uint64_t count_min_sketch::suggest_num_hashes(double confidence){
//    /*
//     * Function to help users select a number of hashes for a given confidence
//     * eg confidence is 1 - failure probability
//     * failure probability == delta in the literature.
//     * * TODO: Change this when update is improved
//     */
//    if(confidence < 0. || confidence > 1.0){
//      throw std::invalid_argument( "Confidence must be between 0 and 1.0 (inclusive)." );
//    }
//    return ceil(log(1.0/(1.0 - confidence))) ;
//  }

} /* namespace datasketches */

#endif