cmake_minimum_required(VERSION 3.0.0)
project(dataTransfer VERSION 0.1.0 LANGUAGES C CXX)

list(APPEND CMAKE_MODULE_PATH "/home/fernando/Área\ de\ Trabalho/Data_Transfer")

#-- Seta flags para o compilador
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -lpqxx -lpq -lfmt")
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(CTest)
enable_testing()

add_executable(dataTransfer main.cpp)

#-- Parteresponsavel pela lib pqxx
find_package(PostgreSQL REQUIRED)
include_directories(${PostgreSQL_INCLUDE_DIRS})
link_directories(${PostgreSQL_LIBRARY_DIRS})

find_package(PQXX REQUIRED)
target_include_directories(dataTransfer PUBLIC ${PQXX_INCLUDE_DIRS})
target_link_libraries(dataTransfer ${PQXX_LIBRARIES})

set(CPACK_PROJECT_NAME ${PROJECT_NAME})
set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
include(CPack)
