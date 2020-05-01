This is a C++ version of the DataSketches core library. See [Apache DataSketches home](http://datasketches.apache.org/)

Apache DataSketches is an open source, high-performance library of stochastic streaming algorithms commonly called "sketches" in the data sciences. Sketches are small, stateful programs that process massive data as a stream and can provide approximate answers, with mathematical guarantees, to computationally difficult queries orders-of-magnitude faster than traditional, exact methods.

This code requires C++11. It was tested with GCC 4.8.5 (standard in RedHat at the time of this writing), GCC 8.2.0 and Apple LLVM version 10.0.1 (clang-1001.0.46.4)

This includes Python bindings. For the Python interface, see the README notes in [the python subdirectory](https://github.com/apache/incubator-datasketches-cpp/tree/master/python).

This library is header-only. The build process provided is only for building unit tests and the python library.

Building the unit tests requires cmake 3.12.0 or higher.

Installing the latest cmake on OSX: brew install cmake

Building and running unit tests using cmake for OSX and Linux:

	$ mkdir build
	$ cd build
	$ cmake ..
	$ make
	$ make test

Building and running unit tests using cmake for Windows from the command line:

  $ mkdir build
	$ cd build
	$ cmake ..
	$ cd ..
	$ cmake --build build --config Release
	$ cmake --build build --config Release --target RUN_TESTS

## How to Contact Us
* We have two ASF [the-ASF.slack.com](http://the-ASF.slack.com) slack channels:
    * datasketches -- general user questions
    * datasketches-dev -- similar to our Apache [Developers Mail list](https://lists.apache.org/list.html?dev@datasketches.apache.org), except more interactive, but not as easily searchable.

* For bugs and performance issues please subscribe: [Issues for datasketches-cpp](https://github.com/apache/incubator-datasketches-cpp/issues) 

* For general questions about using the library please subscribe: [Users Mail List](https://lists.apache.org/list.html?users@datasketches.apache.org)

* If you are interested in contributing please subscribe: [Developers Mail list](https://lists.apache.org/list.html?dev@datasketches.apache.org)