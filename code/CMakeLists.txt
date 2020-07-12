cmake_minimum_required(VERSION 3.10)  # CMake version check
project(presto)

set(CMAKE_CXX_STANDARD 14)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror -Wno-unused -fno-omit-frame-pointer -O0")

set(SOURCE_FILES main.cc metadata.h metadata.cc cache.h hashmap.cc hashmap.h cache.cc util.h policy.cc policy.h
        analyze.cc analyze.h util.cc experiments.cc experiments.h bits.cc bits.h snapshot.cc snapshot.h workload.h
        exp1.cc exp1.h exp2.cc exp2.h exp4.cc exp4.h exp3.cc exp3.h cache-ss.cc)

include_directories("/home/skrtbhtngr/CLionProjects/presto/zlib-1.2.11")
link_directories("/home/skrtbhtngr/CLionProjects/presto/zlib-1.2.11")
#include_directories("/usr/local/opt/openssl/include")
#link_directories("/usr/local/opt/openssl/lib")

link_libraries("ssl")
link_libraries("crypto")
link_libraries("z")

set(THREADS_PREFER_PTHREAD_FLAG ON)
link_libraries("pthread")

# Add executable target with source files listed in SOURCE_FILES variable
add_executable(presto ${SOURCE_FILES})