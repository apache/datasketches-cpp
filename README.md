This is a C++ version of the DataSketches core library. See [Apache DataSketches home](http://datasketches.apache.org/)

Apache DataSketches is an open source, high-performance library of stochastic streaming algorithms commonly called "sketches" in the data sciences. Sketches are small, stateful programs that process massive data as a stream and can provide approximate answers, with mathematical guarantees, to computationally difficult queries orders-of-magnitude faster than traditional, exact methods.

This code requires C++11. It was tested with GCC 4.8.5 (standard in RedHat at the time of this writing), GCC 8.2.0 and Apple LLVM version 10.0.1 (clang-1001.0.46.4)

This includes Python bindings. For the Python interface, see the README notes in [the python subdirectory](https://github.com/apache/incubator-datasketches-cpp/tree/master/python).

This library was intended to be header-only, but this goal was not fully
achieved yet with CPC sketch code. This work is in progress.

Building and running unit tests requires CppUnit.

Installing CppUnit on OSX: brew install cppunit

Installing CppUnit on RHEL: yum install cppunit-devel

There are currently two ways of building: using existing make files and generating
make files using cmake. Exsisting make files might not work on all platforms
or with all C++ compilers. Generating make files using cmake should solve
this problem, but it currently requires cmake version 3.12.0 or later that might not
be readily available as a package on all platforms.

Installing the latest cmake on OSX: brew install cmake

Building and running unit tests using existing make files:

	$ make
	$ make test

Building and running unit tests using cmake:

	$ mkdir build
	$ cd build
	$ cmake ..
	$ make
	$ make test
