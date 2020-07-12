# Source code for PRESTO

This is a CMake project and the IDE used was CLion. `CMakeLists.txt` should be used as it is.


The project is written in C/C++, and Python3 scripts were used for analyzing result csv files and generating graph plots. Target system was a Linux (x64) machine.

--

A custom workload generation tools is also included (`gen.py`, `gen_main.py`).

`conf/` contains sample configuration files for workload generation tool

`results/` contains the results (csv files + graph plots) for 3 workloads used in experiments

`traces/` contains all the block I/O traces used in the project (the workload dataset)

`main.cc` contains the driver function and `util.h` has some configurable parameters defined.

Refer to the Stage II report in `/mtpdocs` for design of the cache and hashmaps.

The directory structure should be maintained as it is. Absolute paths are used in most places which must be changed accordingly.