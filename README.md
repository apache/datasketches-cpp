This is an unfinished effort to port some of datasketches-java code to C++.

We will be standardizing on C++11 for the time being.

For the python interface, clone the repo with `--recursive` to ensure you get the python binding library ([pybind11](https://github.com/pybind/pybind11))
```
git clone --recursive https://github.com/apache/incubator-datasketches-cpp.git
cd incubator-datasketches-cpp
pip install .
```

In the event you do not have `pip` installed, you can invoke the setup script directly by replacing the last line above with `python3 setup.py install`.
