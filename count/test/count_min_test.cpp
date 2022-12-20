#include <catch2/catch.hpp>
#include <vector>

#include "count_min.hpp"
#include "common_defs.hpp"

namespace datasketches{

TEST_CASE("CM init"){
    uint64_t n_hashes = 3, n_buckets = 5, seed = 1234567 ;
    count_min_sketch c(n_hashes, n_buckets, seed) ;
    std::vector<uint64_t> sk_config{n_hashes, n_buckets, seed} ;
    REQUIRE(c.get_num_hashes() == n_hashes) ;
    REQUIRE(c.get_num_buckets() == n_buckets) ;
    REQUIRE(c.get_seed() == seed) ;
    REQUIRE(c.get_config() == sk_config) ;

    std::vector<uint64_t> sketch_table = c.get_sketch() ;
    for(auto x: sketch_table){
      REQUIRE(x == 0) ;
    }

    // Check the default seed is appropriately set.
    count_min_sketch c1(n_hashes, n_buckets) ;
    REQUIRE(c1.get_seed() == DEFAULT_SEED) ;
}

TEST_CASE("CM one update: uint64_t"){
    uint64_t n_hashes = 3, n_buckets = 5, seed = 1234567 ;
    count_min_sketch c(n_hashes, n_buckets, seed) ;
    std::string x = "x" ;
    c.update(x.c_str()) ; // pointer to the string
    std::vector<uint64_t> c_sketch = c.get_sketch() ;

    // init the hash function to check the sketch is appropriately set.
//    HashState hashes ;
//    MurmurHash3_x64_128(x.c_str(), 64, seed, hashes);
//    uint64_t bucket_id, bucket_to_set ;
//    uint64_t hash_location = hashes.h1 ;
//    std::cout<< "Hash seed: " << seed << std::endl;
//    for(uint64_t nh=0; nh < n_hashes; ++nh){
//      hash_location += (nh * hashes.h2) ;
//      bucket_id = hash_location % n_buckets ;
//      std::cout << "hash: " << hash_location << std::endl ;
//      //std::cout << bucket_id << std::endl;
//      bucket_to_set = (nh*n_hashes) + bucket_id ;
//      //std::cout << "The hash location should be: " << bucket_to_set << std::endl;
//      REQUIRE(c_sketch[ bucket_to_set] == 1) ;
//
//    uint64_t est = c.get_estimate(x.c_str()) ;
//    REQUIRE(est == 1) ;
//    }




}



} /* namespace datasketches */

