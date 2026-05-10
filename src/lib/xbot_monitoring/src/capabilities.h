#pragma once

#include "nlohmann/json.hpp"

inline const nlohmann::ordered_json CAPABILITIES = {
    {"rpc", 1},
    {"map:json", 1},
    {"timetable:json", 1},
    {"timetable:bson", 1},
    {"timetable:set:json", 1},
    {"timetable:set:bson", 1},
    {"timetable:renew:json", 1},
    {"timetable:renew:bson", 1},
    {"timetable:validation:json", 1},
    {"timetable:validation:bson", 1},
    {"timetable:suspension:set:json", 1},
    {"timetable:suspension:set:bson", 1},
    {"mqtt:params", 1},
};
