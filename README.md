This is an unfinished effort to port some of datasketches-java code to C++.

We will be standardizing on C++11 for the time being.

For the python interface, clone the repo with `--recursive` to ensure you get the python binding library ([pybind11](https://github.com/pybind/pybind11))
```
git clone --recursive https://github.com/apache/incubator-datasketches-cpp.git
pip install .
```

Since this is still experimental, you may prefer to instead run `python3 config.py develop` to avoid installing the package.
