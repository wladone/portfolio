cmake_minimum_required(VERSION 3.15)
project(engine_stub LANGUAGES CXX)

add_executable(engine_stub ../engine_stub.cpp)
target_compile_features(engine_stub PRIVATE cxx_std_17)
if (WIN32)
  target_link_libraries(engine_stub ws2_32)
endif()
