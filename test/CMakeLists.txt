# Tests need to be added as executables first
add_executable(unittest_fault_tolerance unittest_fault_tolerance.cc)

target_compile_features(unittest_fault_tolerance PRIVATE cxx_std_17)

# Should be linked to the main library
target_link_libraries(unittest_fault_tolerance PRIVATE faulttolerance kvcg_stuff fabricBased)

# If you register a test, then ctest and make test will run it.
# You can also run examples and check the output, as well.
#add_test(NAME testlibtest COMMAND testlib) # Command can be a target

