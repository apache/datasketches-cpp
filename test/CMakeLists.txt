add_subdirectory(hll)
add_subdirectory(kll)
add_executable(
    test_runner
    test_runner.cpp
)
target_link_libraries(
    test_runner
    sketchescore
    hlltest
    klltest
    cppunit
)
add_custom_target(tests COMMENT "Running CPPUNIT tests...")
add_custom_command(TARGET tests COMMAND test_runner)
add_custom_command(
  TARGET test_runner POST_BUILD
  COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/hll/*.bin ${CMAKE_CURRENT_BINARY_DIR}/hll/
  COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/kll/*.bin ${CMAKE_CURRENT_BINARY_DIR}/kll/
)
