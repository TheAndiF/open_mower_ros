cmake_minimum_required(VERSION 3.0.2)
project(mower_timetable)

add_compile_options(-std=c++17)

find_package(catkin REQUIRED COMPONENTS
        roscpp
        std_msgs
        std_srvs
        mower_msgs
        xbot_rpc
        )

catkin_package()

if(NOT TARGET nlohmann_json::nlohmann_json)
        add_subdirectory(../lib/json json EXCLUDE_FROM_ALL)
endif()

include_directories(
        ${catkin_INCLUDE_DIRS}
)

add_executable(timetable_service src/timetable_service.cpp)
add_dependencies(timetable_service ${catkin_EXPORTED_TARGETS})
target_link_libraries(timetable_service ${catkin_LIBRARIES} nlohmann_json::nlohmann_json xbot_rpc)
