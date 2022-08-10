/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <iostream>
#include <iomanip>
#include <set>
#include <catch2/catch.hpp>
#include <tuple_sketch.hpp>
#include <tuple_union.hpp>
#include <stdexcept>

template<typename T>
class max_value_policy {
public:
    max_value_policy(const T& initial_value): initial_value(initial_value) {}
    T create() const { return initial_value; }
    void update(T& summary, const T& update) const { summary = std::max(summary, update); }
private:
    T initial_value;
};

using max_float_update_tuple_sketch = datasketches::update_tuple_sketch<float, float, max_value_policy<float>>;

template<typename T>
class always_one_policy {
public:
    always_one_policy(): initial_value(1) {}
    T create() const { return 1; }
    void update(T& summary, const T& update) const { }
private:
    T initial_value;
};
using always_one_tuple_sketch = datasketches::update_tuple_sketch<int, int, always_one_policy<int>> ;

template<typename T>
class update_sum_value_policy {
public:
    //update_sum_value_policy(const T& initial_value): initial_value(0) {}
    //T create() const { return initial_value; }
    update_sum_value_policy(): initial_value(0) {}
    T create() const { return initial_value; }
    void update(T& summary, const T& update) const { summary += update; }
private:
    T initial_value;
};
using sum_update_tuple_sketch = datasketches::update_tuple_sketch<int, int, update_sum_value_policy<int>>;


// This policy is only for a union sketch, not an update sketch.
// Need another class if the same type of update is gonna work on an update sketch
// with the create and update methods.

template<typename Summary>
struct union_sum_value_policy {
    void operator()(Summary& summary, const Summary& other) const {
        summary += other;
    }
};

using sum_union_tuple_sketch = datasketches::tuple_union<int,  union_sum_value_policy<int>> ;


class EngagementTest{
public:
    int num_std_dev = 2 ;
    void test_always_one_update(){
        std::cout << "########## Testing ALWAYS ONE policy ##########" << std::endl ;
        int lgK = 8 ;
        // Here is where the IntegerSketch should go
        std::vector<datasketches::update_tuple_sketch<int, int, always_one_policy<int>>> sketch_array ;

        auto always_one_sketch = always_one_tuple_sketch::builder(always_one_policy<int>()).set_lg_k(lgK).build() ;

        always_one_sketch.update(1, 1);
        always_one_sketch.update(1, 2);
        always_one_sketch.update(2, 1);
        always_one_sketch.update(3, 3);
        always_one_sketch.update(3, 7);

        int num_retained = 0;
        int sum = 0;
        for (const auto& entry: always_one_sketch) {
            sum += entry.second;
            ++num_retained;
        }
        REQUIRE(num_retained == 3);
        REQUIRE(sum == 3); // we only keep 1 for every stored key.
    }

    void test_sum_update_policy(){
        std::cout << "########## Testing SUM policy on UPDATE SKETCH ##########" << std::endl ;
        int lgK = 8 ;
        auto sum_sketch = sum_update_tuple_sketch::builder().set_lg_k(lgK).build() ;

        sum_sketch.update(1, 1);
        sum_sketch.update(1, 2);
        sum_sketch.update(2, 1);
        sum_sketch.update(3, 3);
        sum_sketch.update(3, 7);
        int num_retained = 0;
        int sum = 0;
        for (const auto& entry: sum_sketch) {
            sum += entry.second;
            ++num_retained;
        }
        REQUIRE(num_retained == 3);
        REQUIRE(sum == 14); // (1+2) + 1 + (3 + 7) = 14
    }

    void test_sum_union_policy(){
        // Union two update sketches using the sum policy
        auto sketch1 = sum_update_tuple_sketch::builder().build() ;
        auto sketch2 = sum_update_tuple_sketch::builder().build() ;

        sketch1.update(1, 1);
        sketch1.update(2, 1);
        sketch1.update(3, 3);
        // std::cout << "********** sketch 1 UPDATE SUMMARY **********" << std::endl ;
        // std::cout << sketch1.to_string(true) << std::endl ;

        sketch2.update(1, 2);
        sketch2.update(2, 1);
        sketch2.update(3, 7);
        // std::cout << "********** sketch 2 UPDATE SUMMARY **********" << std::endl ;
        // std::cout << sketch2.to_string(true) << std::endl ;

        auto union_sketch = sum_union_tuple_sketch::builder().build() ;
        union_sketch.update(sketch1) ;
        union_sketch.update(sketch2) ;
        auto union_result = union_sketch.get_result() ;

        int num_retained = 0;
        int sum = 0;
        for (const auto& entry: union_result) {
            sum += entry.second;
            ++num_retained;
        }
        REQUIRE(num_retained == 3);
        REQUIRE(sum == 15); // 1:(1+2) + 2:(1+1) + 3:(3+7) = 15

        // std::cout << "********** UNION SUMMARY **********" << std::endl ;
        // std::cout << union_result.to_string(true) << std::endl ;

    }

    void compute_engagement_histogram(){
        std::cout << "########## Testing ENGAGEMENT ##########" << std::endl ;
        int lgK = 8 ;
        //int K = 1 << lgK ;
        const int days = 30 ;
        int v = 0 ;
        std::set<int> set_array[days];
        std::vector<datasketches::update_tuple_sketch<int, int, always_one_policy<int>>> sketch_array ;


        for(int i=0; i<days ; i++){

            auto builder = always_one_tuple_sketch::builder(always_one_policy<int>()) ;
            builder.set_lg_k(lgK) ;
            auto sketch = builder.build() ;
            sketch_array.push_back(sketch);
        }
        REQUIRE(sketch_array.size() == days) ;
        std::cout << "Size of vector: " << sketch_array.size() << std::endl ;


        for(int i=0; i<=days; i++){
            int32_t num_ids = get_num_ids(days, i) ;
            int32_t num_days = get_num_days(days, i) ;

            //std::cout << i << "\t" << num_ids << "\t" << num_days << std::endl ;
            // TO DO: Continue from here and figure out what to do with the tuple sketches.
            int my_v = v++ ;
            for(int d=0 ; d<num_days; d++){
                for(int id = 0; id < num_ids; id++){
                    //std::cout << id << " " << my_v << " " << my_v + id << " " << num_ids << std::endl ;
                    set_array[d].insert(my_v + id) ;
                    sketch_array[d].update(my_v + id, 1) ;
                    //std::cout << "d: " << d << " id: " << id << " my_v + id: " << my_v + id << std::endl ;
                    // sk_arr[d].update(my_v + id, 1) ; // update the day d sketch with the key my_v + id
                }
            }
            v += num_ids ;
        }
        union_ops(lgK, sketch_array) ;
    }
private:
    int32_t get_num_ids(int total_days, int index){
        /*
         * Generates power law distributed synthetic data
         */
        double d = total_days ;
        double i = index ;
        return int(round(exp(i * log(d) / d))) ;
    }

    int32_t get_num_days(int total_days, int index){
        double d = total_days ;
        double i = index ;
        return int(round(exp( (d-i) * log(d) / d ))) ;
    }

    int8_t round_double_to_int(double x){
        return int(std::round(x)) ;
    }


    //void union_ops(int lgk, std::vector<datasketches::update_tuple_sketch<int>> sketches){
    void union_ops(int lgk, std::vector<datasketches::update_tuple_sketch<int, int, always_one_policy<int>>> sketches){
        int num_sketches = sketches.size() ;
        //auto u =  datasketches::tuple_union<int>::builder().set_lg_k(lgk).build() ;
        auto u = sum_union_tuple_sketch::builder().set_lg_k(lgk).build() ;


        for(auto sk:sketches){
            u.update(sk) ;
        }
        auto union_result = u.get_result() ;
        std::cout << "Union type: " << typeid(union_result).name() << std::endl ;
        // std::cout << "The estimate is: " << res.get_estimate() << std::endl ; // agrees with 271.9156532434 from java.
        std::vector<uint64_t> num_days_arr(num_sketches+1) ;

        int num_retained = 0 ;
        int total_sum = 0 ;

        for (const auto& entry: union_result) {
            std::cout << "First: " << entry.first << "\tSecond: " << entry.second << std::endl ;
            int num_days_visited = entry.second ;
            num_retained++ ;
            total_sum += entry.second ;
            num_days_arr[num_days_visited]++;
        }
        std::cout << "Num retained items: " << num_retained << std::endl ; // This agrees with Java.
        std::cout << "Sum(retained items): " << total_sum << std::endl ; // This agrees with Java.

        for(int i = 1; i<num_sketches+1; i++){
            std::cout<< "i = " << i << "\tnum_days_arr[i] = " << num_days_arr[i] << std::endl ;
        }

        // *********************** WE AGREE UP TO HERE ***********************
        // For pretty printing
        int sum_visits = 0;
        double theta = union_result.get_theta();
        std::cout <<"\t\tEngagement Histogram.\t\t\t\n" ;
        std::cout << "Number of Unique Visitors by Number of Days Visited" << std::endl ;
        std::cout << "---------------------------------------------------" << std::endl ;

        std::cout << std::setw(12) << "Days Visited"
                  << std::setw(12) << "Estimate"
                  << std::setw(12) << "LB"
                  << std::setw(12) << "UB"
                  << std:: endl ;

        for (uint64_t i = 0; i < num_days_arr.size(); i++) {
            int visitors_at_days_visited = num_days_arr[i] ;
            if(visitors_at_days_visited == 0){ continue; }
            sum_visits += visitors_at_days_visited * i ;
            // RUNS UP TO HERE 10/08/2022

            double est_visitors_at_days_visited = visitors_at_days_visited / theta ;
            double lower_bound_at_days_visited =  union_result.get_lower_bound(num_std_dev, visitors_at_days_visited);
            double upper_bound_at_days_visited =  union_result.get_upper_bound(num_std_dev, visitors_at_days_visited);

            std::cout << std::setw(12) << i
                      << std::setw(12) << est_visitors_at_days_visited
                      << std::setw(12) << lower_bound_at_days_visited
                      << std::setw(12) << upper_bound_at_days_visited
                      << std:: endl ;

    }

        // PRINT A SUMMARY TABLE FOR VISITORS AND VISITS
        std::cout << std::endl << std::endl ;
        std::cout << std::setw(12) << "Totals"
                  << std::setw(12) << "Estimate"
                  << std::setw(12) << "LB"
                  << std::setw(12) << "UB"
                  << std:: endl ;
        std::cout << "---------------------------------------------------" << std::endl ;

        const double total_visitors = union_result.get_estimate() ;
        const double lb_visitors = union_result.get_lower_bound(num_std_dev) ;
        const double ub_visitors = union_result.get_upper_bound(num_std_dev) ;


        std::cout << std::setw(12) << "Visitors"
                  << std::setw(12) << total_visitors
                  << std::setw(12) << lb_visitors
                  << std::setw(12) << ub_visitors
                  << std:: endl ;

        //The total number of visits, however, is a scaled metric and takes advantage of the fact that
        //the retained entries in the sketch is a uniform random sample of all unique visitors, and
        //the the rest of the unique users will likely behave in the same way.
        const double est_visits = sum_visits / theta;
        const double lb_visits = est_visits * lb_visitors / total_visitors;
        const double ub_visits = est_visits * ub_visitors / total_visitors;


        std::cout << std::setw(12) << "Visits"
                  << std::setw(12) << est_visits
                  << std::setw(12) << lb_visits
                  << std::setw(12) << ub_visits
                  << std:: endl ;
    }

};

namespace datasketches {

    TEST_CASE("engagement", "[engagement]") {
    EngagementTest E ;
    E.test_always_one_update() ;
    E.test_sum_update_policy() ;
    E.test_sum_union_policy() ;
    E.compute_engagement_histogram() ;
}


} /* namespace datasketches */


