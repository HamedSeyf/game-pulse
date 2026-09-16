
# for CXX_MODULES build
cmake_minimum_required(VERSION 3.28)
project(game_pulse LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_UNITY_BUILD OFF)

add_subdirectory(submodules/hamed_common)
add_subdirectory(submodules/spdlog)

add_executable(game_pulse
	src/main.cpp
	src/implementation/queue.cpp
	src/implementation/pipeline.cpp
	src/implementation/analytics.cpp
	src/implementation/reporting.cpp
	src/implementation/simulator.cpp
)

target_link_libraries(game_pulse
	PRIVATE
		hamed_common
        spdlog::spdlog
)

# Compile-time floor for SPDLOG_TRACE/DEBUG/INFO/WARN/ERROR/CRITICAL: calls below this
# level compile out entirely instead of being a runtime no-op, so profiling builds can
# lower it (e.g. to SPDLOG_LEVEL_OFF) to remove spdlog's overhead completely.
target_compile_definitions(game_pulse
	PRIVATE
		SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG
)

target_sources(game_pulse
	PRIVATE
		FILE_SET CXX_MODULES
		FILES
			src/modules/game_pulse.domain.ixx
			src/modules/game_pulse.queue.ixx
			src/modules/game_pulse.pipeline.ixx
			src/modules/game_pulse.analytics.ixx
			src/modules/game_pulse.reporting.ixx
			src/modules/game_pulse.simulator.ixx
)

if (MSVC)
  target_compile_options(game_pulse PRIVATE /W4 /WX)
else()
  target_compile_options(game_pulse PRIVATE -Wall -Wextra -Wpedantic -Werror)
endif()
