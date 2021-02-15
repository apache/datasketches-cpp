# Python Wrapper for Apache DataSketches

## Installation

The release files do not include the needed python binding library ([pybind11](https://github.com/pybind/pybind11)). If building
from a relase package, you must ensure that the pybind11 directory points to a local copy of pybind11.

An official pypi build is eventually planned but not yet available.

If you instead want to take a (possibly ill-advised) gamble on the current state of the master branch being useable, you can run:
```pip install git+https://github.com/apache/datasketches-cpp.git```

## Developer Instructions

### Building

When cloning the source repository, you should include the pybind11 submodule with the `--recursive` option to the clone command:
```
git clone --recursive https://github.com/apache/datasketches-cpp.git
cd datasketches-cpp
python -m pip install --upgrade pip setuptools wheel numpy
python setup.py build
```

If you cloned without `--recursive`, you can add the submodule post-checkout using `git submodule update --init --recursive`.

### Installing

Assuming you have already checked out the library and any dependent submodules, install by simply replacing the lsat
line of the build command with `python setup.py install`.

### Unit tests

The python tests are run with `tox`. To ensure you have all the needed packages, from the package base directory run:
```
python -m pip install --upgrade pip setuptools wheel numpy tox
tox
```

## Usage

Having installed the library, loading the Apache Datasketches library in Python is simple: `import datasketches`.

## Available Sketch Classes

- KLL (Absolute Error Quantiles)
    - `kll_ints_sketch`
    - `kll_floats_sketch`
- REQ (Relative Error Quantiles)
    - `req_ints_sketch`
    - `req_floats_sketch`
- Frequent Items
    - `frequent_strings_sketch`
    - Error types are `frequent_items_error_type.{NO_FALSE_NEGATIVES | NO_FALSE_POSITIVES}`
- Theta
    - `update_theta_sketch`
    - `compact_theta_sketch` (cannot be instantiated directly)
    - `theta_union`
    - `theta_intersection`
    - `theta_a_not_b`
- HLL
    - `hll_sketch`
    - `hll_union`
    - Target HLL types are `tgt_hll_type.{HLL_4 | HLL_6 | HLL_8}`
- CPC
    - `cpc_sketch`
    - `cpc_union`
- VarOpt Sampling
    - `var_opt_sketch`
    - `var_opt_union`
- Vector of KLL
    - `vector_of_kll_ints_sketches`
    - `vector_of_kll_floats_sketches`

## Known Differences from C++

The Python API largely mirrors the C++ API, with a few minor exceptions: The primary known differences are that Python on modern platforms does not support unsigned integer values or numeric values with fewer than 64 bits. As a result, you may not be able to produce identical sketches from within Python as you can with Java and C++. Loading those sketches after they have been serialized from another language will work as expected.

The Vector of KLL object is currently exclusive to python, and holds an array of independent KLL sketches. This is useful for creating a set of KLL sketches over a vector and has been designed to allow input as either a vector or a matrix of multiple vectors.

We have also removed reliance on a builder class for theta sketches as Python allows named arguments to the constructor, not strictly positional arguments.
