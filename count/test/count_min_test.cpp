#include <catch2/catch.hpp>
#include <vector>

#include "count_min.hpp"
#include "common_defs.hpp"

namespace datasketches{

TEST_CASE("CM init"){
    uint64_t n_hashes = 3, n_buckets = 5, seed = 1234567 ;
    count_min_sketch<uint64_t> c(n_hashes, n_buckets, seed) ;
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
    count_min_sketch<uint64_t> c1(n_hashes, n_buckets) ;
    REQUIRE(c1.get_seed() == DEFAULT_SEED) ;
}

TEST_CASE("CM parameter suggestions", "[error parameters]") {

    // Bucket suggestions
    REQUIRE_THROWS(count_min_sketch<uint64_t>::suggest_num_buckets(-1.0), "Confidence must be between 0 and 1.0 (inclusive)." ) ;
    REQUIRE(count_min_sketch<uint64_t>::suggest_num_buckets(0.2) == 14) ;
    REQUIRE(count_min_sketch<uint64_t>::suggest_num_buckets(0.1) == 28) ;
    REQUIRE(count_min_sketch<uint64_t>::suggest_num_buckets(0.05) == 55) ;
    REQUIRE(count_min_sketch<uint64_t>::suggest_num_buckets(0.01) == 272) ;

    // Check that the sketch get_epsilon acts inversely to suggest_num_buckets
    uint64_t n_hashes = 3 ;
    REQUIRE(count_min_sketch<uint64_t>(n_hashes, 14).get_relative_error() <= 0.2) ;
    REQUIRE(count_min_sketch<uint64_t>(n_hashes, 28).get_relative_error() <= 0.1) ;
    REQUIRE(count_min_sketch<uint64_t>(n_hashes, 55).get_relative_error() <= 0.05) ;
    REQUIRE(count_min_sketch<uint64_t>(n_hashes, 272).get_relative_error() <= 0.01) ;

    // Hash suggestions
    REQUIRE_THROWS(count_min_sketch<uint64_t>::suggest_num_hashes(10.0), "Confidence must be between 0 and 1.0 (inclusive)." ) ;
    REQUIRE_THROWS(count_min_sketch<uint64_t>::suggest_num_hashes(-1.0), "Confidence must be between 0 and 1.0 (inclusive)." ) ;
    REQUIRE(count_min_sketch<uint64_t>::suggest_num_hashes(0.682689492) == 2) ; // 1 STDDEV
    REQUIRE(count_min_sketch<uint64_t>::suggest_num_hashes(0.954499736) == 4) ; // 2 STDDEV
    REQUIRE(count_min_sketch<uint64_t>::suggest_num_hashes(0.997300204) == 6) ; // 3 STDDEV
}

TEST_CASE("CM one update: uint64_t"){
    uint64_t n_hashes = 3, n_buckets = 5, seed = 1234567 ;
    uint64_t inserted_weight = 0 ;
    count_min_sketch<uint64_t> c(n_hashes, n_buckets, seed) ;
    std::string x = "x" ;

    REQUIRE(c.get_estimate("x") == 0) ; // No items in sketch so estimates should be zero
    c.update(x) ;
    REQUIRE(c.get_estimate(x) == 1) ;
    inserted_weight += 1 ;

    uint64_t w = 9 ;
    inserted_weight += w ;
    c.update(x, w) ;
    REQUIRE(c.get_estimate(x) == inserted_weight) ;

    // Doubles are converted to uint64_t
    double w1 = 10.0 ;
    inserted_weight += w1 ;
    c.update(x, w1) ;
    REQUIRE(c.get_estimate(x) == inserted_weight) ;
    REQUIRE(c.get_total_weight() == inserted_weight) ;
    REQUIRE(c.get_estimate(x) <= c.get_upper_bound(x)) ;
    REQUIRE(c.get_estimate(x) >= c.get_lower_bound(x)) ;
}

//   // Check that the hash locations are set correctly.
//    std::vector<uint64_t> c_sketch = c.get_sketch() ;
//    // init the hash function to check the sketch is appropriately set.
//    HashState hashes ;
//    MurmurHash3_x64_128(x.c_str(), x.length(),  seed, hashes);
//    uint64_t bucket_id, bucket_to_set ;
//    uint64_t hash_location = hashes.h1 ;
//    //std::cout<< "Hash seed: " << seed << std::endl;
//    for(uint64_t nh=0; nh < n_hashes; ++nh){
//      hash_location += (nh * hashes.h2) ;
//      bucket_id = hash_location % n_buckets ;
//      //std::cout << "hash: " << hash_location << std::endl ;
//      //std::cout << bucket_id << std::endl;
//      bucket_to_set = (nh*n_hashes) + bucket_id ;
//      //std::cout << "The hash location should be: " << bucket_to_set << std::endl;
//      REQUIRE(c_sketch[bucket_to_set] == 1) ;
//    }




} /* namespace datasketches */

