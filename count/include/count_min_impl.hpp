#ifndef COUNT_MIN_IMPL_HPP_
#define COUNT_MIN_IMPL_HPP_

#include "MurmurHash3.h"

namespace datasketches {
count_min_sketch::count_min_sketch(uint64_t num_hashes, uint64_t num_buckets, uint64_t seed):
num_hashes(num_hashes),
num_buckets(num_buckets),
seed(seed){
  sketch.resize(num_hashes*num_buckets) ;
  //std::cout << "Vector size: " << table.capacity() <<std::endl;
  //sketch[3] = 1 ;
//  for(uint64_t i=0; i<num_hashes*num_buckets; ++i){std::cout << sketch[i] ;}
//  std::cout << std::endl;
};

uint64_t count_min_sketch::get_num_hashes() {
    return num_hashes ;
}

uint64_t count_min_sketch::get_num_buckets() {
    return num_buckets ;
}

uint64_t count_min_sketch::get_seed() {
    return seed ;
}

std::vector<uint64_t> count_min_sketch::get_sketch(){
  return sketch ;
}

std::vector<uint64_t> count_min_sketch::get_config(){
    std::vector<uint64_t> config(3) ;
    config = {get_num_hashes(), get_num_buckets(), get_seed()} ;
    return config ;
} ;

std::vector<uint64_t> count_min_sketch::get_hashes(const void* item){
  /*
   * Returns the hash locations for the input item.
   * Approach taken from
   * https://github.com/Claudenw/BloomFilter/wiki/Bloom-Filters----An-overview
   */
  uint64_t bucket_index ;
  std::vector<uint64_t> sketch_update_locations(num_hashes) ;
  //sketch_update_locations.resize(num_hashes) ;
  HashState hashes;
  std::cout<< "Hash seed: " << seed << std::endl;
  std::cout << "Item: " << item << std::endl;
  MurmurHash3_x64_128(item, 64, seed, hashes); //


  uint64_t hash = hashes.h1 ;
  for(uint64_t hash_idx=0; hash_idx<num_hashes; ++hash_idx){
    hash += (hash_idx * hashes.h2) ;
    bucket_index = hash % num_buckets ;
    std::cout << "HASH " << hash << std::endl ;
    sketch_update_locations.at(hash_idx) = (hash_idx * num_hashes) + bucket_index ;
  }
  return sketch_update_locations ;
}

void count_min_sketch::update(const void* item) {
  /*
   * Gets the value's hash locations and then increments the sketch in those
   * locations.
   */
  std::vector<uint64_t> hash_locations = get_hashes(item) ;
  for (auto h: hash_locations){
    sketch[h] += 1 ;
  }
}

uint64_t count_min_sketch::get_estimate(const void* item){
  /*
   * Returns the estimated frequency of the item
   */
  std::vector<uint64_t> hash_locations = get_hashes(item) ;
  std::vector<uint64_t> estimates ;
  for (auto h: hash_locations){
    std::cout<< h << " " << sketch[h] << std::endl;
    estimates.push_back(sketch[h]) ;
  }
  uint64_t result = *std::min_element(estimates.begin(), estimates.end());
  return result ;
}

} /* namespace datasketches */

#endif