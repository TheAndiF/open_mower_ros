// Created by Clemens Elflein on 2/18/22, 5:37 PM.
// Copyright (c) 2022 Clemens Elflein and OpenMower contributors. All rights reserved.
//
// This file is part of OpenMower.
//
// OpenMower is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
// License as published by the Free Software Foundation, version 3 of the License.
//
// OpenMower is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along with OpenMower. If not, see
// <https://www.gnu.org/licenses/>.
//
#include "grid_map_cv/GridMapCvConverter.hpp"
#include "grid_map_ros/GridMapRosConverter.hpp"
#include "grid_map_ros/PolygonRosConverter.hpp"
#include "ros/ros.h"
#include "std_msgs/String.h"
#include "visualization_msgs/MarkerArray.h"

// Rosbag for reading/writing the map to a file
#include <rosbag/bag.h>
#include <rosbag/view.h>

// Include Messages
#include "geometry_msgs/Point32.h"
#include "geometry_msgs/Polygon.h"
#include "mower_map/MapArea.h"

// Include Service Messages
#include "mower_map/AddMowingAreaSrv.h"
#include "mower_map/ClearMapSrv.h"
#include "mower_map/ClearNavPointSrv.h"
#include "mower_map/GetDockingPointSrv.h"
#include "mower_map/GetMowingAreaSrv.h"
#include "mower_map/SetDockingPointSrv.h"
#include "mower_map/SetNavPointSrv.h"

// JSON for map storage
#include <algorithm>
#include <cmath>
#include <cfloat>
#include <filesystem>
#include <fstream>
#include <limits>
#include <nlohmann/json.hpp>
#include <random>
#include <set>
#include <string>
#include <vector>
using json = nlohmann::ordered_json;

// Monitoring
#include <tf2_geometry_msgs/tf2_geometry_msgs.h>

#include "xbot_msgs/MapSize.h"

// RPC
#include "xbot_rpc/provider.h"

const std::string MAP_FILE = "map.json";
const std::string LEGACY_MAP_FILE = "map.bag";

// Forward declarations
void saveMapToFile();
void buildMap();
bool normalizeAreaProperties();
bool hasValidMowingOrder();
bool recalculateMowingOrder();
bool prepareMapAreasOnStartup();
void updatePersistentMap(bool recalculate_order);

// Struct definitions for JSON serialization
struct Point {
  double x;
  double y;
};

typedef std::vector<Point> Polygon;

struct MapArea {
  std::string id;
  std::string name;
  std::string type;
  bool active = true;
  bool mowing_enabled = true;
  int mowing_order = 0;
  Polygon outline;
};

struct DockingStation {
  std::string id;
  std::string name;
  bool active;
  Point position;
  double heading;
};

struct MapData {
  std::vector<MapArea> areas;
  std::vector<DockingStation> docking_stations;

  std::vector<MapArea> getMowingAreas() {
    std::vector<MapArea> result;
    for (const auto& area : areas) {
      if (area.type != "mow") continue;
      if (!area.active) continue;
      if (!area.mowing_enabled) continue;
      if (area.mowing_order <= 0) continue;
      result.push_back(area);
    }

    std::sort(result.begin(), result.end(), [](const MapArea& a, const MapArea& b) {
      return a.mowing_order < b.mowing_order;
    });

    return result;
  }

  void clear() {
    areas.clear();
    docking_stations.clear();
  }

  std::string toJsonString();
};

// JSON serialization macros
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Point, x, y)

void to_json(json& j, const MapArea& data) {
  j["id"] = data.id;

  json properties = json::object();
  properties["type"] = data.type;
  properties["name"] = data.name.empty() ? data.id : data.name;

  if (data.type == "mow") {
    properties["mowing_enabled"] = data.mowing_enabled;
    properties["mowing_order"] = data.mowing_order;
  } else {
    properties["mowing_enabled"] = false;
    properties["mowing_order"] = 0;
  }

  if (!data.active) properties["active"] = data.active;

  j["properties"] = properties;
  j["outline"] = data.outline;
}

void from_json(const json& j, MapArea& data) {
  j.at("id").get_to(data.id);
  const auto& properties = j.value("properties", json::object());

  data.type = properties.value("type", "draft");
  data.active = properties.value("active", true);

  if (properties.contains("name") && !properties["name"].is_null()) {
    data.name = properties["name"].get<std::string>();
  } else {
    data.name = "";
  }

  data.mowing_enabled = properties.value("mowing_enabled", data.type == "mow");
  data.mowing_order = properties.value("mowing_order", 0);

  j.at("outline").get_to(data.outline);
}

void to_json(json& j, const DockingStation& data) {
  j["id"] = data.id;
  json properties = json::object();
  if (!data.name.empty()) properties["name"] = data.name;
  if (!data.active) properties["active"] = data.active;
  if (!properties.empty()) j["properties"] = properties;
  j["position"] = data.position;
  j["heading"] = data.heading;
}

void from_json(const json& j, DockingStation& data) {
  j.at("id").get_to(data.id);
  const auto& properties = j.value("properties", json::object());
  data.name = properties.value("name", "");
  data.active = properties.value("active", true);
  j.at("position").get_to(data.position);
  j.at("heading").get_to(data.heading);
}

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(MapData, areas, docking_stations)

std::string MapData::toJsonString() {
  json json_data = *this;
  return json_data.dump(2);
}

std::string generateNanoId(size_t length = 32) {
  static const char alphabet[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  thread_local std::mt19937 rng{std::random_device{}()};
  thread_local std::uniform_int_distribution<> dist(0, sizeof(alphabet) - 2);
  std::string id(length, '\0');
  std::generate_n(id.begin(), length, [&]() { return alphabet[dist(rng)]; });
  return id;
}

// Publishes the map as JSON string
ros::Publisher json_map_pub;

// Publishes the map as occupancy grid
ros::Publisher map_pub;

// Publishes the map as markers for rviz
ros::Publisher map_server_viz_array_pub;

// Publishes map size for heatmap generator
ros::Publisher map_size_pub;

// MapData instance - the source of truth for map data
MapData map_data;

bool show_fake_obstacle = false;
geometry_msgs::Pose fake_obstacle_pose;

// The grid map. This is built from the polygons loaded from the file.
grid_map::GridMap map;

// clang-format off
xbot_rpc::RpcProvider rpc_provider("mower_map_service", {{
  RPC_METHOD("map.replace", {
    if (!params.is_array() || params.size() != 1) {
      throw xbot_rpc::RpcException(xbot_rpc::RpcError::ERROR_INVALID_PARAMS, "Missing map parameter");
    }
    try {
      map_data = params[0];
    } catch (const std::exception& e) {
      throw xbot_rpc::RpcException(xbot_rpc::RpcError::ERROR_INVALID_PARAMS, "Invalid map: " + std::string(e.what()));
    }
    updatePersistentMap(true);
    ROS_INFO_STREAM("Loaded " << map_data.areas.size() << " areas via RPC, recalculated mowing order and saved to file");
    return "Successfully stored map (" + std::to_string(map_data.areas.size()) + " areas)";
  }),
}});
// clang-format on

/**
 * Convert a geometry_msgs::Polygon to our internal Polygon struct
 */
Polygon geometryPolygonToInternal(const geometry_msgs::Polygon& poly) {
  Polygon result;
  for (const auto& point : poly.points) {
    result.push_back({point.x, point.y});
  }
  return result;
}

/**
 * Convert our internal Polygon struct to geometry_msgs::Polygon
 */
geometry_msgs::Polygon internalPolygonToGeometry(const Polygon& poly) {
  geometry_msgs::Polygon result;
  for (const auto& point : poly) {
    geometry_msgs::Point32 pt;
    pt.x = point.x;
    pt.y = point.y;
    result.points.push_back(pt);
  }
  return result;
}

/**
 * Convert a mower_map::MapArea to our internal MapArea struct
 */
MapArea mowerMapAreaToInternal(const geometry_msgs::Polygon& area, const std::string& type, const std::string& name) {
  MapArea result;
  result.id = generateNanoId();
  result.type = type;
  result.name = name;
  result.active = true;
  result.mowing_enabled = (type == "mow");
  result.mowing_order = 0;
  result.outline = geometryPolygonToInternal(area);
  return result;
}

/**
 * Convert our internal MapArea struct to mower_map::MapArea
 */
mower_map::MapArea internalMapAreaToMower(const MapArea& area) {
  mower_map::MapArea result;
  result.name = area.name;
  // Leave area empty if it is not active
  if (area.active) {
    result.area = internalPolygonToGeometry(area.outline);
  }
  return result;
}

grid_map::Polygon internalPolygonToGridMap(const Polygon& poly) {
  grid_map::Polygon result;
  for (const auto& point : poly) {
    result.addVertex(grid_map::Position(point.x, point.y));
  }
  return result;
}

double squaredDistance(const Point& a, const Point& b) {
  const double dx = a.x - b.x;
  const double dy = a.y - b.y;
  return dx * dx + dy * dy;
}

Point polygonCentroid(const Polygon& poly) {
  if (poly.empty()) return {0.0, 0.0};

  double x = 0.0;
  double y = 0.0;
  for (const auto& p : poly) {
    x += p.x;
    y += p.y;
  }

  return {x / poly.size(), y / poly.size()};
}

bool pointInPolygon(const Point& point, const Polygon& poly) {
  if (poly.size() < 3) return false;

  bool inside = false;
  for (size_t i = 0, j = poly.size() - 1; i < poly.size(); j = i++) {
    const Point& pi = poly[i];
    const Point& pj = poly[j];

    const bool intersects = ((pi.y > point.y) != (pj.y > point.y)) &&
                            (point.x < (pj.x - pi.x) * (point.y - pi.y) / ((pj.y - pi.y) + 1e-12) + pi.x);
    if (intersects) inside = !inside;
  }

  return inside;
}

double distancePointToSegmentSquared(const Point& point, const Point& a, const Point& b) {
  const double dx = b.x - a.x;
  const double dy = b.y - a.y;
  const double length_sq = dx * dx + dy * dy;

  if (length_sq <= 1e-12) return squaredDistance(point, a);

  double t = ((point.x - a.x) * dx + (point.y - a.y) * dy) / length_sq;
  t = std::max(0.0, std::min(1.0, t));

  const Point projection{a.x + t * dx, a.y + t * dy};
  return squaredDistance(point, projection);
}

double distancePointToPolygonSquared(const Point& point, const Polygon& poly) {
  if (poly.empty()) return std::numeric_limits<double>::infinity();
  if (pointInPolygon(point, poly)) return 0.0;

  double best = std::numeric_limits<double>::infinity();
  for (size_t i = 0; i < poly.size(); ++i) {
    const Point& a = poly[i];
    const Point& b = poly[(i + 1) % poly.size()];
    best = std::min(best, distancePointToSegmentSquared(point, a, b));
  }

  return best;
}

double distancePolygonsSquared(const Polygon& a, const Polygon& b) {
  if (a.empty() || b.empty()) return std::numeric_limits<double>::infinity();

  double best = std::numeric_limits<double>::infinity();

  for (const auto& p : a) {
    best = std::min(best, distancePointToPolygonSquared(p, b));
  }
  for (const auto& p : b) {
    best = std::min(best, distancePointToPolygonSquared(p, a));
  }

  return best;
}


// Two polygons are considered connected when they touch/overlap or are closer than this tolerance.
// The tolerance is important because map polygons rarely touch exactly due to GPS/float noise.
constexpr double MOWING_ORDER_CONNECTION_TOLERANCE = 0.30;  // meters

bool polygonsConnected(const Polygon& a, const Polygon& b,
                       double tolerance = MOWING_ORDER_CONNECTION_TOLERANCE) {
  if (a.empty() || b.empty()) return false;
  return distancePolygonsSquared(a, b) <= tolerance * tolerance;
}

bool isActiveNavigationArea(const MapArea& area) {
  return area.active && area.type == "nav" && !area.outline.empty();
}

/**
 * Returns true if two mowing areas are reachable from each other for ordering purposes.
 *
 * Reachable means:
 *  - the mowing areas touch/overlap directly within the configured tolerance, or
 *  - they are connected through one or more active nav areas.
 *
 * Nav areas are not mowed and keep mowing_order=0, but they can act as corridors between mowing areas.
 */
bool mowingAreasConnectedDirectlyOrViaNav(const MapArea& from, const MapArea& to) {
  if (polygonsConnected(from.outline, to.outline)) return true;

  std::vector<const MapArea*> nav_areas;
  for (const auto& area : map_data.areas) {
    if (isActiveNavigationArea(area)) {
      nav_areas.push_back(&area);
    }
  }

  if (nav_areas.empty()) return false;

  std::vector<bool> visited(nav_areas.size(), false);
  std::vector<size_t> stack;

  for (size_t i = 0; i < nav_areas.size(); ++i) {
    if (polygonsConnected(from.outline, nav_areas[i]->outline)) {
      visited[i] = true;
      stack.push_back(i);
    }
  }

  while (!stack.empty()) {
    const size_t current_index = stack.back();
    stack.pop_back();

    const MapArea* current_nav = nav_areas[current_index];
    if (polygonsConnected(current_nav->outline, to.outline)) {
      return true;
    }

    for (size_t i = 0; i < nav_areas.size(); ++i) {
      if (visited[i]) continue;
      if (!polygonsConnected(current_nav->outline, nav_areas[i]->outline)) continue;

      visited[i] = true;
      stack.push_back(i);
    }
  }

  return false;
}

std::vector<MapArea*> getConnectionAnchorsByPriority(MapArea* current_area,
                                                     const std::vector<MapArea*>& ordered) {
  std::vector<MapArea*> anchors;

  if (current_area != nullptr) {
    anchors.push_back(current_area);
  }

  std::vector<MapArea*> previous_areas;
  for (auto* previous_area : ordered) {
    if (previous_area == nullptr) continue;
    if (previous_area == current_area) continue;
    previous_areas.push_back(previous_area);
  }

  if (current_area != nullptr) {
    std::sort(previous_areas.begin(), previous_areas.end(), [&](const MapArea* a, const MapArea* b) {
      return distancePolygonsSquared(current_area->outline, a->outline) <
             distancePolygonsSquared(current_area->outline, b->outline);
    });
  }

  anchors.insert(anchors.end(), previous_areas.begin(), previous_areas.end());
  return anchors;
}

std::vector<MapArea*> calculateOrderedMowingAreas() {
  std::vector<MapArea*> candidates;
  for (auto& area : map_data.areas) {
    if (area.type == "mow" && area.active && area.mowing_enabled && !area.outline.empty()) {
      candidates.push_back(&area);
    }
  }

  std::vector<MapArea*> ordered;
  if (candidates.empty()) return ordered;

  if (map_data.docking_stations.empty()) {
    ROS_WARN_STREAM("No docking station available. Falling back to current enabled mowing area order.");
    return candidates;
  }

  const Point docking_position = map_data.docking_stations.front().position;
  MapArea* current_area = nullptr;

  while (!candidates.empty()) {
    MapArea* next_area = nullptr;

    if (ordered.empty()) {
      // First mowing area: start from the docking station and choose the closest mowing area.
      auto best_it = std::min_element(candidates.begin(), candidates.end(), [&](const MapArea* a, const MapArea* b) {
        return distancePointToPolygonSquared(docking_position, a->outline) <
               distancePointToPolygonSquared(docking_position, b->outline);
      });

      next_area = *best_it;
    } else {
      // Next areas are searched by anchor priority:
      // 1. first from the current / last ordered mowing area,
      // 2. then from the already ordered mowing areas nearest to the current area, one by one.
      // A candidate is only valid for an anchor if it is connected to that anchor directly
      // or through active nav areas. If no anchor has a connected candidate, ordering stops.
      const auto anchors = getConnectionAnchorsByPriority(current_area, ordered);

      for (const auto* anchor : anchors) {
        if (anchor == nullptr) continue;

        std::vector<MapArea*> selection_pool;
        for (auto* candidate : candidates) {
          if (mowingAreasConnectedDirectlyOrViaNav(*anchor, *candidate)) {
            selection_pool.push_back(candidate);
          }
        }

        if (selection_pool.empty()) continue;

        auto best_it = std::min_element(selection_pool.begin(), selection_pool.end(), [&](const MapArea* a, const MapArea* b) {
          return distancePolygonsSquared(anchor->outline, a->outline) <
                 distancePolygonsSquared(anchor->outline, b->outline);
        });

        next_area = *best_it;
        break;
      }

      if (next_area == nullptr) {
        ROS_WARN_STREAM("Stopping mowing order calculation: " << candidates.size()
                        << " enabled mowing area(s) are not connected to the current mowing area "
                        << "or to any previously ordered mowing area directly or through active nav areas.");
        break;
      }
    }

    current_area = next_area;
    ordered.push_back(current_area);

    auto candidate_it = std::find(candidates.begin(), candidates.end(), current_area);
    if (candidate_it != candidates.end()) {
      candidates.erase(candidate_it);
    }
  }

  return ordered;
}

bool normalizeAreaProperties() {
  bool changed = false;

  for (auto& area : map_data.areas) {
    if (area.name.empty()) {
      area.name = area.id;
      changed = true;
    }

    if (area.type == "mow") {
      // Missing mowing_enabled defaults to true in from_json(). Keep existing explicit values.
      continue;
    }

    // Non-mowing areas are never part of the active mowing sequence.
    if (area.mowing_enabled != false) {
      area.mowing_enabled = false;
      changed = true;
    }

    if (area.mowing_order != 0) {
      area.mowing_order = 0;
      changed = true;
    }
  }

  return changed;
}

bool hasValidMowingOrder() {
  std::set<int> used_orders;
  size_t mow_area_count = 0;

  for (const auto& area : map_data.areas) {
    if (area.type != "mow") continue;
    if (area.outline.empty()) continue;

    ++mow_area_count;

    if (area.mowing_order <= 0) {
      ROS_WARN_STREAM("Mowing area '" << area.name << "' (" << area.id
                      << ") has missing/invalid mowing_order=" << area.mowing_order);
      return false;
    }

    if (!used_orders.insert(area.mowing_order).second) {
      ROS_WARN_STREAM("Duplicate mowing_order=" << area.mowing_order
                      << " found at mowing area '" << area.name << "' (" << area.id << ")");
      return false;
    }
  }

  if (mow_area_count == 0) return true;
  return true;
}

bool recalculateMowingOrder() {
  bool changed = false;

  // Clear all existing order values first. Areas that cannot be connected to the ordered
  // component must not keep stale mowing_order values from an older map state.
  for (auto& area : map_data.areas) {
    if (area.mowing_order != 0) {
      area.mowing_order = 0;
      changed = true;
    }
  }

  int order = 10;
  for (auto* area : calculateOrderedMowingAreas()) {
    if (area->mowing_order != order) {
      area->mowing_order = order;
      changed = true;
    }
    order += 10;
  }

  return changed;
}

/**
 * Startup behavior:
 *  - Normalize area properties.
 *  - Keep an existing valid mowing_order exactly as stored in map.json.
 *  - Recalculate only if the stored mowing_order is missing, duplicate or otherwise invalid.
 */
bool prepareMapAreasOnStartup() {
  bool changed = normalizeAreaProperties();

  if (!hasValidMowingOrder()) {
    ROS_WARN_STREAM("Stored mowing_order is missing or invalid. Recalculating mowing order once on startup.");
    changed = recalculateMowingOrder() || changed;
  } else {
    ROS_INFO_STREAM("Stored mowing_order is valid. Keeping existing mowing order.");
  }

  return changed;
}

/**
 * Publish map to xbot_monitoring
 */
void publishMapMonitoring() {
  xbot_msgs::MapSize map_size;
  map_size.mapWidth = map.getSize().x() * map.getResolution();
  map_size.mapHeight = map.getSize().y() * map.getResolution();
  auto mapPos = map.getPosition();
  map_size.mapCenterX = mapPos.x();
  map_size.mapCenterY = mapPos.y();
  map_size_pub.publish(map_size);

  std_msgs::String json_map;
  json_map.data = map_data.toJsonString();
  json_map_pub.publish(json_map);
}

/**
 * Publish map visualizations for rviz.
 */
void visualizeAreas() {
  auto mapPos = map.getPosition();

  visualization_msgs::MarkerArray markerArray;

  for (const auto& area : map_data.areas) {
    if (!area.active) continue;
    if (area.type != "mow" && area.type != "obstacle") continue;

    std_msgs::ColorRGBA color;
    if (area.type == "mow") {
      color.g = 1.0;
    } else if (area.type == "obstacle") {
      color.r = 1.0;
    }
    color.a = 1.0;

    grid_map::Polygon p = internalPolygonToGridMap(area.outline);
    visualization_msgs::Marker marker;
    grid_map::PolygonRosConverter::toLineMarker(p, color, 0.05, 0, marker);

    marker.header.frame_id = "map";
    marker.ns = "mower_map_service";
    marker.id = markerArray.markers.size();
    marker.frame_locked = true;
    marker.pose.orientation.w = 1.0;

    markerArray.markers.push_back(marker);
  }

  // Visualize Docking Point
  if (!map_data.docking_stations.empty()) {
    const DockingStation& ds = map_data.docking_stations.front();
    geometry_msgs::Pose docking_pose;
    docking_pose.position.x = ds.position.x;
    docking_pose.position.y = ds.position.y;
    docking_pose.position.z = 0.0;

    double heading = ds.heading;
    tf2::Quaternion q;
    q.setRPY(0.0, 0.0, heading);
    docking_pose.orientation = tf2::toMsg(q);

    std_msgs::ColorRGBA color;
    color.b = 1.0;
    color.a = 1.0;
    visualization_msgs::Marker marker;

    marker.action = visualization_msgs::Marker::ADD;
    marker.scale.x = 0.2;
    marker.scale.y = 0.05;
    marker.scale.z = 0.05;
    marker.color = color;
    marker.type = visualization_msgs::Marker::ARROW;
    marker.pose = docking_pose;
    ROS_INFO_STREAM("docking pose: " << docking_pose);
    marker.header.frame_id = "map";
    marker.ns = "mower_map_service";
    marker.id = markerArray.markers.size() + 1;
    marker.frame_locked = true;
    markerArray.markers.push_back(marker);
  }

  map_server_viz_array_pub.publish(markerArray);
}

/**
 * Uses the polygons stored in MapData to build the final occupancy grid.
 *
 * First, the map is marked as completely occupied. Then navigation_areas and mowing_areas are marked as free.
 *
 * Then, all obstacles are marked as occupied.
 *
 * Finally, a blur is applied to the map so that it is expensive, but not completely forbidden to drive near boundaries.
 */
void buildMap() {
  // First, calculate the size of the map by finding the min and max values for x and y.
  float minX = FLT_MAX;
  float maxX = -FLT_MAX;
  float minY = FLT_MAX;
  float maxY = -FLT_MAX;

  // loop through all areas and calculate a size where everything fits
  bool has_valid_area = false;
  for (const auto& area : map_data.areas) {
    if (!area.active) continue;
    if (area.type != "mow" && area.type != "nav" && area.type != "obstacle") continue;
    for (const auto& point : area.outline) {
      minX = std::min(minX, (float)point.x);
      maxX = std::max(maxX, (float)point.x);
      minY = std::min(minY, (float)point.y);
      maxY = std::max(maxY, (float)point.y);
      has_valid_area = true;
    }
  }

  // Enlarge the map by 1m in all directions.
  // This guarantees that even after blurring, the map has an occupied border.
  maxX += 1.0;
  minX -= 1.0;
  maxY += 1.0;
  minY -= 1.0;

  // Check, if the map was empty. If so, we'd create a huge map. Therefore we build an empty 10x10m map instead.
  if (!has_valid_area) {
    maxX = 5.0;
    minX = -5.0;
    maxY = 5.0;
    minY = -5.0;
  }

  map = grid_map::GridMap({"navigation_area"});
  map.setFrameId("map");
  grid_map::Position origin;
  origin.x() = (maxX + minX) / 2.0;
  origin.y() = (maxY + minY) / 2.0;

  ROS_INFO_STREAM("Map Position: x=" << origin.x() << ", y=" << origin.y());
  ROS_INFO_STREAM("Map Size: x=" << (maxX - minX) << ", y=" << (maxY - minY));

  map.setGeometry(grid_map::Length(maxX - minX, maxY - minY), 0.05, origin);
  map.setTimestamp(ros::Time::now().toNSec());

  map.clearAll();
  map["navigation_area"].setConstant(1.0);

  grid_map::Matrix& data = map["navigation_area"];
  for (const auto& area : map_data.areas) {
    if (!area.active) continue;

    if (area.type == "mow" || area.type == "nav") {
      grid_map::Polygon poly = internalPolygonToGridMap(area.outline);
      for (grid_map::PolygonIterator iterator(map, poly); !iterator.isPastEnd(); ++iterator) {
        const grid_map::Index index(*iterator);
        data(index[0], index[1]) = 0.0;
      }
    }
  }

  for (const auto& area : map_data.areas) {
    if (!area.active) continue;

    if (area.type == "obstacle") {
      grid_map::Polygon poly = internalPolygonToGridMap(area.outline);
      for (grid_map::PolygonIterator iterator(map, poly); !iterator.isPastEnd(); ++iterator) {
        const grid_map::Index index(*iterator);
        data(index[0], index[1]) = 1.0;
      }
    }
  }

  if (show_fake_obstacle) {
    grid_map::Polygon poly;
    tf2::Quaternion q;
    tf2::fromMsg(fake_obstacle_pose.orientation, q);

    tf2::Matrix3x3 m(q);
    double unused1, unused2, yaw;

    m.getRPY(unused1, unused2, yaw);

    Eigen::Vector2d front(cos(yaw), sin(yaw));
    Eigen::Vector2d left(-sin(yaw), cos(yaw));
    Eigen::Vector2d obstacle_pos(fake_obstacle_pose.position.x, fake_obstacle_pose.position.y);

    {
      grid_map::Position pos = obstacle_pos + 0.1 * left + 0.25 * front;
      poly.addVertex(pos);
    }
    {
      grid_map::Position pos = obstacle_pos + 0.2 * left - 0.1 * front;
      poly.addVertex(pos);
    }
    {
      grid_map::Position pos = obstacle_pos + 0.6 * left - 0.1 * front;
      poly.addVertex(pos);
    }
    {
      grid_map::Position pos = obstacle_pos + 0.6 * left + 0.7 * front;
      poly.addVertex(pos);
    }

    {
      grid_map::Position pos = obstacle_pos - 0.6 * left + 0.7 * front;
      poly.addVertex(pos);
    }
    {
      grid_map::Position pos = obstacle_pos - 0.6 * left - 0.1 * front;
      poly.addVertex(pos);
    }
    {
      grid_map::Position pos = obstacle_pos - 0.2 * left - 0.1 * front;
      poly.addVertex(pos);
    }
    {
      grid_map::Position pos = obstacle_pos - 0.1 * left + 0.25 * front;
      poly.addVertex(pos);
    }
    for (grid_map::PolygonIterator iterator(map, poly); !iterator.isPastEnd(); ++iterator) {
      const grid_map::Index index(*iterator);
      data(index[0], index[1]) = 1.0;
    }
  }

  cv::Mat cv_map;
  grid_map::GridMapCvConverter::toImage<unsigned char, 1>(map, "navigation_area", CV_8UC1, cv_map);

  cv::blur(cv_map, cv_map, cv::Size(5, 5));

  grid_map::GridMapCvConverter::addLayerFromImage<unsigned char, 1>(cv_map, "navigation_area", map);

  nav_msgs::OccupancyGrid msg;
  grid_map::GridMapRosConverter::toOccupancyGrid(map, "navigation_area", 0.0, 1.0, msg);
  map_pub.publish(msg);

  publishMapMonitoring();
  visualizeAreas();
}

/**
 * Saves the current map data to a JSON file.
 * We don't need to save the grid map, since we can easily build it again after loading.
 */
void saveMapToFile() {
  std::ofstream file(MAP_FILE);
  if (file.is_open()) {
    file << map_data.toJsonString();
    file.close();
    ROS_INFO("Map saved to JSON file");
  } else {
    ROS_ERROR("Failed to open JSON file for writing");
  }
}

/**
 * Normalize persistent map data, save it and rebuild all derived map outputs.
 * Use this after changes that should be stored in map.json.
 */
void updatePersistentMap(bool recalculate_order) {
  normalizeAreaProperties();

  if (recalculate_order) {
    recalculateMowingOrder();
  } else if (!hasValidMowingOrder()) {
    ROS_WARN_STREAM("Persistent map update detected invalid mowing_order. Recalculating mowing order.");
    recalculateMowingOrder();
  }

  saveMapToFile();
  buildMap();
}

/**
 * Load the map from a JSON file and build a map.
 */
void readMapFromFile() {
  std::ifstream json_file(MAP_FILE);
  if (json_file.is_open()) {
    try {
      json loaded_data;
      json_file >> loaded_data;
      json_file.close();

      map_data = loaded_data;

      ROS_INFO_STREAM("Loaded " << map_data.areas.size() << " areas from: " << MAP_FILE);
    } catch (const std::exception& e) {
      ROS_ERROR_STREAM("Failed to parse " << MAP_FILE << ": " << e.what());
    }
  } else {
    ROS_WARN_STREAM("Could not open map file: " << MAP_FILE);
  }
}

bool addMowingArea(mower_map::AddMowingAreaSrvRequest& req, mower_map::AddMowingAreaSrvResponse& res) {
  ROS_INFO_STREAM("Got addMowingArea call");

  map_data.areas.push_back(mowerMapAreaToInternal(req.area.area, req.isNavigationArea ? "nav" : "mow", req.area.name));
  for (const auto& obstacle : req.area.obstacles) {
    map_data.areas.push_back(mowerMapAreaToInternal(obstacle, "obstacle", ""));
  }

  updatePersistentMap(true);
  return true;
}

bool getMowingArea(mower_map::GetMowingAreaSrvRequest& req, mower_map::GetMowingAreaSrvResponse& res) {
  ROS_INFO_STREAM("Got getMowingArea call with index: " << req.index);

  auto mowing_areas = map_data.getMowingAreas();
  if (req.index >= mowing_areas.size()) {
    ROS_ERROR_STREAM("No mowing area with index: " << req.index);
    return false;
  }

  const auto& selected_area = mowing_areas[req.index];
  ROS_INFO_STREAM("Selected mowing area: " << selected_area.name << " order=" << selected_area.mowing_order);
  res.area = internalMapAreaToMower(selected_area);

  for (const auto& area : map_data.areas) {
    if (!area.active || area.type != "obstacle") continue;
    res.area.obstacles.push_back(internalPolygonToGeometry(area.outline));
  }

  return true;
}

bool setDockingPoint(mower_map::SetDockingPointSrvRequest& req, mower_map::SetDockingPointSrvResponse& res) {
  ROS_INFO_STREAM("Setting Docking Point");

  // Convert quaternion to heading
  tf2::Quaternion q;
  tf2::fromMsg(req.docking_pose.orientation, q);
  tf2::Matrix3x3 m(q);
  double unused1, unused2, heading;
  m.getRPY(unused1, unused2, heading);

  map_data.docking_stations.clear();
  map_data.docking_stations.push_back({.id = generateNanoId(),
                                       .name = "Docking Station",
                                       .active = true,
                                       .position = {req.docking_pose.position.x, req.docking_pose.position.y},
                                       .heading = heading});

  updatePersistentMap(true);

  return true;
}

bool getDockingPoint(mower_map::GetDockingPointSrvRequest& req, mower_map::GetDockingPointSrvResponse& res) {
  ROS_INFO_STREAM("Getting Docking Point");

  if (map_data.docking_stations.empty()) {
    return false;
  }

  const DockingStation& ds = map_data.docking_stations.front();
  res.docking_pose.position.x = ds.position.x;
  res.docking_pose.position.y = ds.position.y;
  res.docking_pose.position.z = 0.0;

  tf2::Quaternion q;
  q.setRPY(0.0, 0.0, ds.heading);
  res.docking_pose.orientation = tf2::toMsg(q);

  return true;
}

bool setNavPoint(mower_map::SetNavPointSrvRequest& req, mower_map::SetNavPointSrvResponse& res) {
  ROS_INFO_STREAM("Setting Nav Point");

  fake_obstacle_pose = req.nav_pose;

  show_fake_obstacle = true;

  buildMap();

  return true;
}

bool clearNavPoint(mower_map::ClearNavPointSrvRequest& req, mower_map::ClearNavPointSrvResponse& res) {
  ROS_INFO_STREAM("Clearing Nav Point");

  if (show_fake_obstacle) {
    show_fake_obstacle = false;

    buildMap();
  }

  return true;
}

bool clearMap(mower_map::ClearMapSrvRequest& req, mower_map::ClearMapSrvResponse& res) {
  ROS_INFO_STREAM("Clearing Map");

  map_data.clear();

  updatePersistentMap(true);
  return true;
}

/**
 * Helper function to convert legacy map areas from bag file
 * @param bag The rosbag to read from
 * @param topic_name The topic name to query (e.g. "mowing_areas" or "navigation_areas")
 * @param area_type The type to assign to the converted areas (e.g. "mow" or "nav")
 */
void convertLegacyAreas(rosbag::Bag& bag, const std::string& topic_name, const std::string& area_type) {
  rosbag::View view(bag, rosbag::TopicQuery(topic_name));
  for (rosbag::MessageInstance const m : view) {
    auto area = m.instantiate<mower_map::MapArea>();
    if (area) {
      // Convert main area
      MapArea main_area;
      main_area.id = generateNanoId();
      main_area.name = area->name;
      main_area.type = area_type;
      main_area.active = true;
      main_area.outline = geometryPolygonToInternal(area->area);
      map_data.areas.push_back(main_area);

      // Convert obstacles as separate areas
      for (const auto& obstacle : area->obstacles) {
        MapArea obs_area;
        obs_area.id = generateNanoId();
        obs_area.name = "";
        obs_area.type = "obstacle";
        obs_area.active = true;
        obs_area.outline = geometryPolygonToInternal(obstacle);
        map_data.areas.push_back(obs_area);
      }
    }
  }
}

void convertLegacyMapToJson() {
  // Open the legacy map file
  rosbag::Bag bag;
  try {
    bag.open(LEGACY_MAP_FILE);
  } catch (rosbag::BagIOException& e) {
    ROS_ERROR_STREAM("Error opening legacy map file for conversion: " << e.what());
    return;
  }

  // Clear the current map_data
  map_data.clear();

  // Read mowing and navigation areas
  convertLegacyAreas(bag, "mowing_areas", "mow");
  convertLegacyAreas(bag, "navigation_areas", "nav");

  // Read docking point
  {
    rosbag::View view(bag, rosbag::TopicQuery("docking_point"));
    for (rosbag::MessageInstance const m : view) {
      auto pt = m.instantiate<geometry_msgs::Pose>();
      if (pt) {
        // Convert quaternion to yaw
        tf2::Quaternion q;
        tf2::fromMsg(pt->orientation, q);
        tf2::Matrix3x3 m(q);
        double unused1, unused2, yaw;
        m.getRPY(unused1, unused2, yaw);

        // Create docking station
        DockingStation ds;
        ds.id = generateNanoId();
        ds.name = "Docking Station";
        ds.active = true;
        ds.position = {pt->position.x, pt->position.y};
        ds.heading = yaw;
        map_data.docking_stations.push_back(ds);
      }
    }
  }

  bag.close();

  // Save the converted data to JSON file. Legacy conversion creates a new persistent map, so calculate the order.
  normalizeAreaProperties();
  recalculateMowingOrder();
  saveMapToFile();

  ROS_INFO_STREAM("Successfully converted legacy map to JSON with "
                  << map_data.areas.size() << " areas and " << map_data.docking_stations.size() << " docking stations");
}

int main(int argc, char** argv) {
  ros::init(argc, argv, "mower_map_service");
  ros::NodeHandle n;
  json_map_pub = n.advertise<std_msgs::String>("mower_map_service/json_map", 1, true);
  map_pub = n.advertise<nav_msgs::OccupancyGrid>("mower_map_service/map", 10, true);
  map_server_viz_array_pub = n.advertise<visualization_msgs::MarkerArray>("mower_map_service/map_viz", 10, true);
  map_size_pub = n.advertise<xbot_msgs::MapSize>("mower_map_service/map_size", 10, true);

  rpc_provider.init();

  if (!std::filesystem::exists(MAP_FILE) && std::filesystem::exists(LEGACY_MAP_FILE)) {
    ROS_INFO("Found legacy map file, converting to JSON...");
    convertLegacyMapToJson();
  } else {
    readMapFromFile();
  }

  if (prepareMapAreasOnStartup()) {
    saveMapToFile();
  }

  buildMap();

  ros::ServiceServer add_area_srv = n.advertiseService("mower_map_service/add_mowing_area", addMowingArea);
  ros::ServiceServer get_area_srv = n.advertiseService("mower_map_service/get_mowing_area", getMowingArea);
  ros::ServiceServer set_docking_point_srv = n.advertiseService("mower_map_service/set_docking_point", setDockingPoint);
  ros::ServiceServer get_docking_point_srv = n.advertiseService("mower_map_service/get_docking_point", getDockingPoint);
  ros::ServiceServer set_nav_point_srv = n.advertiseService("mower_map_service/set_nav_point", setNavPoint);
  ros::ServiceServer clear_nav_point_srv = n.advertiseService("mower_map_service/clear_nav_point", clearNavPoint);
  ros::ServiceServer clear_map_srv = n.advertiseService("mower_map_service/clear_map", clearMap);

  ros::spin();
  return 0;
}
