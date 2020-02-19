# Use C++17
set(CMAKE_CXX_STANDARD 17)

# Use C99
set(CMAKE_C_STANDARD 99)

add_executable(ceaflate
    main.cpp
)

target_link_libraries(ceaflate z pthread)
