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

TEST_CASE("Cm merge operation"){
    double relative_error = 0.25 ;
    double confidence = 0.9 ;
    uint64_t n_buckets = count_min_sketch<uint64_t>::suggest_num_buckets(relative_error) ;
    uint64_t n_hashes = count_min_sketch<uint64_t>::suggest_num_hashes(confidence) ;
    std::cout << "Buckets: " << n_buckets << "\tHashes: " << n_hashes << std::endl;

    count_min_sketch<uint64_t> s(n_hashes, n_buckets, 9082435234709287) ;


    // Generate sketches that we cannot merge into ie they disagree on at least one of the config entries
    // TODO: implement more general merge procedure so different hashes or buckets are permitted.
    count_min_sketch<uint64_t> s1(n_hashes+1, n_buckets) ; // incorrect number of hashes
    count_min_sketch<uint64_t> s2(n_hashes, n_buckets+1) ;// incorrect number of buckets
    count_min_sketch<uint64_t> s3(n_hashes, n_buckets, 1) ;// incorrect seed
    std::vector<count_min_sketch<uint64_t>> sketches = {s1, s2, s3};

    // Fail cases
    REQUIRE_THROWS(s.merge(s), "Cannot merge a sketch with itself." ) ;
    for(count_min_sketch<uint64_t> sk : sketches){
      REQUIRE_THROWS(s.merge(sk), "Incompatible sketch config." ) ;
    }

    // Passing case
    std::vector<uint64_t> s_config = s.get_config() ; // Construct a sketch with correct configuration.
    count_min_sketch<uint64_t> t(s_config[0], s_config[1], s_config[2]) ;
    //s.merge(t) ;

    std::vector<uint64_t> data = {2, 3, 5, 7, 11};//
    for(auto d: data){
      s.update(d) ;
      t.update(d) ;
    }
    std::vector<uint64_t> s_sk = s.get_sketch() ;
    std::vector<uint64_t> t_sk = t.get_sketch() ;
    for(uint64_t ii=0 ; ii < (n_buckets*n_hashes); ++ii){
      std::cout << ii << "\t" << s_sk[ii] << "\t" << t_sk[ii] << std::endl;
    }
    //s.merge(t);


    //REQUIRE(s.get_total_weight() == 2*t.get_total_weight());
    std::cout << "Estimation checks: " << std::endl ;
    for (auto x : data) {
      std::cout << x << "|\t" << s.get_estimate(x) << "\t" << t.get_estimate(x) << std::endl;
      //REQUIRE(s.get_estimate(ii) <= 2); // I don't think this line is quite correct.
    }

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

