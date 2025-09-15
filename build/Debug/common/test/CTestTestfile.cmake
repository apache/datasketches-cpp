# CMake generated Testfile for 
# Source directory: /Users/andrea.novellini/Code/datasketches/datasketches-cpp/common/test
# Build directory: /Users/andrea.novellini/Code/datasketches/datasketches-cpp/build/Debug/common/test
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(common_test "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/build/Debug/common/test/common_test")
set_tests_properties(common_test PROPERTIES  _BACKTRACE_TRIPLES "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/common/test/CMakeLists.txt;63;add_test;/Users/andrea.novellini/Code/datasketches/datasketches-cpp/common/test/CMakeLists.txt;0;")
add_test(integration_test "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/build/Debug/common/test/integration_test")
set_tests_properties(integration_test PROPERTIES  _BACKTRACE_TRIPLES "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/common/test/CMakeLists.txt;85;add_test;/Users/andrea.novellini/Code/datasketches/datasketches-cpp/common/test/CMakeLists.txt;0;")
subdirs("../../_deps/catch2-build")
