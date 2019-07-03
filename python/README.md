# Python Wrapper for Datasketches

## Usage

Having followed installation instructions in the root directory, loading the Datasketches library in Python is simple: `from datasketches import *`.

## Available Sketch Classes

- KLL
    - `kll_ints_sketch`
    - `kll_floats_sketch`
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
- CPC
    - `cpc_sketch`
    - `cpc_union`

## Known Differences from C++

The Python API largely mirrors the C++ API, with a few minor exceptions: The primary known differences are that Python on modern platforms does not support unsigned integer values or numeric values with fewer than 64 bits. As a result, you may not be able to produce identical sketches from within Python as you can with Java and C++. Loading those sketches after they have been serialized from another language will work as expected.

We have also removed reliance on a builder class for theta sketches as Python allows named arguments to the constructor, not strictly positional arguments.
