#include <catch2/catch.hpp>

#include "count_min.hpp"

namespace datasketches{

TEST_CASE("CM init"){
    uint64_t n_hashes = 5 ;
    count_min_sketch c(n_hashes) ;
    REQUIRE(c.get_num_hashes() == 5) ;
}

} /* namespace datasketches */

