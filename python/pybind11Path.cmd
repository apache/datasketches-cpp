@echo off
:: Takes path to the Python interpreter and returns the path to pybind11
%1 -c "import pybind11,sys;sys.stdout.write(pybind11.get_cmake_dir())"