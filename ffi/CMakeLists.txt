cmake_minimum_required(VERSION 3.0)

#---------------------------------------------------------------------------------------------------
# Banyan CFFI Library
#---------------------------------------------------------------------------------------------------
cargo_build(NAME banyan-ffi)

target_link_libraries(banyan-ffi
	INTERFACE
		pthread
		dl
)

target_include_directories(banyan-ffi
	INTERFACE
		$<INSTALL_INTERFACE:include>
		$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)

#---------------------------------------------------------------------------------------------------
# Banyan CFFI Demos
#---------------------------------------------------------------------------------------------------
add_executable(coordinator examples/coordinator.cpp)
target_link_libraries(coordinator banyan-ffi)

add_executable(worker examples/worker.c)
target_link_libraries(worker banyan-ffi)

#---------------------------------------------------------------------------------------------------
# Installation
#---------------------------------------------------------------------------------------------------
install(TARGETS banyan-ffi
	EXPORT banyan-targets
	LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
	ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
install(DIRECTORY include/
	DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
