# Apache DataSketches Core C++ Library Component
This is the core C++ component of the Apache DataSketches library.  It contains all of the key sketching algorithms that are in the Java component and can be accessed directly from user applications. 

This component is also a dependency of other components of the library that create adaptors for target systems, such as PostgreSQL.

Note that we have a parallel core component for Java implementations of the same sketch algorithms, 
[datasketches-java](https://github.com/apache/datasketches-java).

Please visit the main [Apache DataSketches website](https://datasketches.apache.org) for more information. 

If you are interested in making contributions to this site please see our [Community](https://datasketches.apache.org/docs/Community/) page for how to contact us.

---

This code requires C++11.

This includes Python bindings. For the Python interface, see the README notes in [the python subdirectory](https://github.com/apache/datasketches-cpp/tree/master/python).

This library is header-only. The build process provided is only for building unit tests and the python library.

Building the unit tests requires cmake 3.12.0 or higher.

Installing the latest cmake on OSX: brew install cmake

Building and running unit tests using cmake for OSX and Linux:

```
	$ cmake -S . -B build/Release -DCMAKE_BUILD_TYPE=Release
	$ cmake --build build/Release -t test
```

Building and running unit tests using cmake for Windows from the command line:

```
	$ cd build
	$ cmake ..
	$ cd ..
	$ cmake --build build --config Release
	$ cmake --build build --config Release --target RUN_TESTS
```

To install a local distribution (OSX and Linux), use the following command. The
CMAKE_INSTALL_PREFIX variable controls the destination. If not specified, it 
defaults to installing in /usr (/usr/include, /usr/lib, etc). In the command below,
the installation will be in /tmp/install/DataSketches (/tmp/install/DataSketches/include,
/tmp/install/DataSketches/lib, etc)

```
	$ cmake -S . -B build/Release -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/tmp/install/DataSketches
	$ cmake --build build/Release -t install
```

To generate an installable package using cmake's built in cpack packaging tool,
use the following command. The type of packaging is controlled by the CPACK_GENERATOR
variable (semi-colon separated list). Cmake usually supports packaging types such as RPM,
DEB, STGZ, TGZ, TZ, ZIP, etc.

```
	$ cmake3 -S . -B build/Release -DCMAKE_BUILD_TYPE=Release -DCPACK_GENERATOR="RPM;STGZ;TGZ" 
	$ cmake3 --build build/Release -t package
```