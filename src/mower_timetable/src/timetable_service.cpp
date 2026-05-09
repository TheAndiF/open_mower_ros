// Timetable service for OpenMower
// Manages the resource "time" similar to the map service managing the resource "area".

#include "mower_msgs/HighLevelControlSrv.h"
#include "mower_msgs/HighLevelStatus.h"
#include "ros/ros.h"
#include "std_msgs/Bool.h"
#include "std_msgs/String.h"
#include "std_srvs/Trigger.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <ctime>
#include <cctype>
#include <fstream>
#include <set>
#include <sstream>
#include <string>
#include <vector>

// RPC, used analog to mower_map_service. This is optional for the first file based test,
// but gives us a ready JSON input path for the later MQTT/RPC wiring.
#include "xbot_rpc/provider.h"

using json = nlohmann::json;

namespace {

constexpr const char* DEFAULT_TIMETABLE_FILE = "/srv/ros/timetable.json";

struct TimetableEntry {
  std::string id;
  std::string day;
  int start_minutes = 0;
  int end_minutes = 0;
  std::string start_text;
  std::string end_text;
  std::string end_behavior = "return_to_dock";
  bool enabled = true;
  bool auto_start = true;
  int minimum_remaining_window_minutes = 30;
  std::string required_battery_state = "full";
};

struct TimetableConfig {
  json raw = json::object();
  std::string timezone = "Europe/Berlin";
  bool time_required = true;
  bool require_valid_time = true;
  bool outside_allow_start = false;
  std::string default_end_behavior = "return_to_dock";
  bool repeat_until_window_end = true;
  bool auto_start = true;
  std::string required_battery_state = "full";
  int minimum_remaining_window_minutes = 30;
  std::vector<TimetableEntry> entries;
};

struct Evaluation {
  bool time_valid = false;
  std::string time_source = "system";
  bool active_window = false;
  bool start_allowed = false;
  bool mission_trigger_allowed = false;
  std::string active_entry_id;
  std::string active_day;
  std::string active_start;
  std::string active_end;
  std::string end_behavior;
  int remaining_window_minutes = 0;
  std::string reason;
};

std::string trim(const std::string& in) {
  const auto first = in.find_first_not_of(" \t\r\n");
  if (first == std::string::npos) return "";
  const auto last = in.find_last_not_of(" \t\r\n");
  return in.substr(first, last - first + 1);
}

bool parseTimeHHMM(const std::string& text, int& minutes) {
  if (text.size() != 5 || text[2] != ':') return false;
  if (!std::isdigit(text[0]) || !std::isdigit(text[1]) || !std::isdigit(text[3]) || !std::isdigit(text[4])) return false;
  const int h = std::stoi(text.substr(0, 2));
  const int m = std::stoi(text.substr(3, 2));
  if (h < 0 || h > 23 || m < 0 || m > 59) return false;
  minutes = h * 60 + m;
  return true;
}

bool isValidDay(const std::string& day) {
  static const std::set<std::string> days = {"Monday", "Tuesday", "Wednesday", "Thursday",
                                             "Friday", "Saturday", "Sunday"};
  return days.count(day) > 0;
}

bool isValidEndBehavior(const std::string& mode) {
  static const std::set<std::string> modes = {"return_to_dock", "finish_current_run", "pause", "stop"};
  return modes.count(mode) > 0;
}

bool isValidRequiredBatteryState(const std::string& state) {
  static const std::set<std::string> states = {"full", "sufficient"};
  return states.count(state) > 0;
}

std::string weekdayName(const std::tm& tm) {
  // tm_wday: Sunday=0, Monday=1, ... Saturday=6
  static const char* days[] = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};
  return days[tm.tm_wday];
}

bool configureProcessTimezone(const std::string& timezone, std::vector<std::string>& remarks) {
  if (timezone.empty()) {
    remarks.emplace_back("time.timezone is missing or empty");
    return false;
  }

  setenv("TZ", timezone.c_str(), 1);
  tzset();
  remarks.emplace_back("Timezone set to " + timezone);
  return true;
}

bool systemTimeLooksValid() {
  const std::time_t now = std::time(nullptr);
  if (now <= 0) return false;
  std::tm local_tm{};
  localtime_r(&now, &local_tm);
  // Treat dates before 2023 as not trustworthy for a mower timetable.
  return (local_tm.tm_year + 1900) >= 2023;
}

}  // namespace

class TimetableService {
 public:
  TimetableService() : nh_(), private_nh_("~"), rpc_provider_("timetable_service", {}) {
    private_nh_.param<std::string>("timetable_file", timetable_file_, DEFAULT_TIMETABLE_FILE);
    private_nh_.param<double>("check_rate_hz", check_rate_hz_, 1.0);
    private_nh_.param<double>("battery_full_threshold", battery_full_threshold_, 0.98);
    private_nh_.param<double>("battery_sufficient_threshold", battery_sufficient_threshold_, 0.30);
    private_nh_.param<int>("default_minimum_remaining_window_minutes", default_minimum_remaining_window_minutes_, 30);
    private_nh_.param<double>("start_command_cooldown_seconds", start_command_cooldown_seconds_, 60.0);
    private_nh_.param<bool>("dry_run", dry_run_, false);
    private_nh_.param<bool>("control_manual_missions_on_window_end", control_manual_missions_on_window_end_, false);

    status_pub_ = nh_.advertise<std_msgs::String>("timetable/status", 1, true);
    start_allowed_pub_ = nh_.advertise<std_msgs::Bool>("timetable/start_allowed", 1, true);
    trigger_allowed_pub_ = nh_.advertise<std_msgs::Bool>("timetable/mission_trigger_allowed", 1, true);

    high_level_sub_ = nh_.subscribe("mower_logic/current_state", 1, &TimetableService::highLevelStatusCallback, this);
    high_level_client_ = nh_.serviceClient<mower_msgs::HighLevelControlSrv>("mower_service/high_level_control");

    reload_srv_ = nh_.advertiseService("timetable/reload", &TimetableService::reloadService, this);

    // RPC method, ready for later MQTT/RPC integration. The first test still works file-based only.
    rpc_provider_.addMethod({"timetable.replace", [this](const std::string&, const nlohmann::basic_json<>& params) {
      if (!params.is_array() || params.size() != 1) {
        throw xbot_rpc::RpcException(xbot_rpc::RpcError::ERROR_INVALID_PARAMS, "Missing timetable parameter");
      }
      std::vector<std::string> remarks;
      TimetableConfig candidate;
      if (!parseTimetableJson(params[0], candidate, remarks)) {
        json response = buildStatusJson(false, remarks, Evaluation{}, nullptr);
        publishStatus(response);
        throw xbot_rpc::RpcException(xbot_rpc::RpcError::ERROR_INVALID_PARAMS, response.dump());
      }
      config_ = candidate;
      valid_ = true;
      remarks_ = remarks;
      saveTimetableToFile();
      evaluateAndPublish();
      return nlohmann::basic_json<>("Timetable stored successfully");
    }});
    rpc_provider_.init();

    reloadFromFile();
    timer_ = nh_.createTimer(ros::Duration(1.0 / std::max(0.1, check_rate_hz_)), &TimetableService::timerCallback, this);
  }

 private:
  bool reloadService(std_srvs::Trigger::Request&, std_srvs::Trigger::Response& res) {
    const bool ok = reloadFromFile();
    res.success = ok;
    res.message = ok ? "Timetable reloaded" : "Timetable reload failed";
    return true;
  }

  void highLevelStatusCallback(const mower_msgs::HighLevelStatus::ConstPtr& msg) {
    last_high_level_status_ = *msg;
    has_high_level_status_ = true;
  }

  void timerCallback(const ros::TimerEvent&) {
    evaluateAndPublish();
  }

  bool reloadFromFile() {
    std::ifstream f(timetable_file_);
    if (!f.good()) {
      valid_ = false;
      remarks_ = {"Cannot open timetable file: " + timetable_file_};
      publishStatus(buildStatusJson(false, remarks_, Evaluation{}, nullptr));
      ROS_ERROR_STREAM("Cannot open timetable file: " << timetable_file_);
      return false;
    }

    json loaded;
    try {
      f >> loaded;
    } catch (const std::exception& e) {
      valid_ = false;
      remarks_ = {"Cannot parse timetable JSON: " + std::string(e.what())};
      publishStatus(buildStatusJson(false, remarks_, Evaluation{}, nullptr));
      ROS_ERROR_STREAM("Cannot parse timetable JSON: " << e.what());
      return false;
    }

    std::vector<std::string> remarks;
    TimetableConfig candidate;
    if (!parseTimetableJson(loaded, candidate, remarks)) {
      valid_ = false;
      remarks_ = remarks;
      publishStatus(buildStatusJson(false, remarks_, Evaluation{}, nullptr));
      ROS_ERROR_STREAM("Invalid timetable: " << json(remarks).dump());
      return false;
    }

    config_ = candidate;
    valid_ = true;
    remarks_ = remarks;
    evaluateAndPublish();
    ROS_INFO_STREAM("Timetable loaded from " << timetable_file_ << " with " << config_.entries.size() << " entries");
    return true;
  }

  bool saveTimetableToFile() {
    std::ofstream f(timetable_file_);
    if (!f.good()) {
      remarks_.push_back("Could not save timetable to file: " + timetable_file_);
      ROS_ERROR_STREAM("Could not save timetable to file: " << timetable_file_);
      return false;
    }
    f << config_.raw.dump(2) << std::endl;
    return true;
  }

  bool parseTimetableJson(const json& root, TimetableConfig& out, std::vector<std::string>& remarks) {
    bool ok = true;
    out = TimetableConfig{};
    out.minimum_remaining_window_minutes = default_minimum_remaining_window_minutes_;
    out.raw = root;

    if (!root.is_object()) {
      remarks.emplace_back("Root must be a JSON object");
      return false;
    }

    if (!root.contains("version")) {
      remarks.emplace_back("version is missing");
      ok = false;
    }

    if (!root.contains("time") || !root["time"].is_object()) {
      remarks.emplace_back("time object is missing");
      ok = false;
    } else {
      const auto& time = root["time"];
      out.timezone = time.value("timezone", "");
      out.time_required = time.value("required", true);
      out.require_valid_time = time.value("require_valid_time", true);
      if (out.timezone.empty()) {
        remarks.emplace_back("time.timezone is missing");
        ok = false;
      }
      if (!configureProcessTimezone(out.timezone, remarks)) ok = false;
    }

    if (root.contains("outside_window")) {
      if (!root["outside_window"].is_object()) {
        remarks.emplace_back("outside_window must be an object keyed by ids");
        ok = false;
      } else {
        bool found_enabled_rule = false;
        for (const auto& item : root["outside_window"].items()) {
          const auto& rule = item.value();
          if (!rule.is_object()) {
            remarks.emplace_back("outside_window." + item.key() + " must be an object");
            ok = false;
            continue;
          }
          if (rule.value("enabled", true)) {
            found_enabled_rule = true;
            out.outside_allow_start = rule.value("allow_start", false);
            break;
          }
        }
        if (!found_enabled_rule) {
          remarks.emplace_back("No enabled outside_window rule found, defaulting allow_start=false");
          out.outside_allow_start = false;
        }
      }
    }

    if (root.contains("on_window_end")) {
      if (!root["on_window_end"].is_object()) {
        remarks.emplace_back("on_window_end must be an object keyed by ids");
        ok = false;
      } else {
        for (const auto& item : root["on_window_end"].items()) {
          const auto& rule = item.value();
          if (!rule.is_object()) {
            remarks.emplace_back("on_window_end." + item.key() + " must be an object");
            ok = false;
            continue;
          }
          if (!rule.value("enabled", true)) continue;
          out.default_end_behavior = rule.value("default_mode", out.default_end_behavior);
          break;
        }
      }
    }

    if (!isValidEndBehavior(out.default_end_behavior)) {
      remarks.emplace_back("on_window_end default_mode has invalid value: " + out.default_end_behavior);
      ok = false;
    }

    if (root.contains("start_policy")) {
      if (!root["start_policy"].is_object()) {
        remarks.emplace_back("start_policy must be an object keyed by ids");
        ok = false;
      } else {
        for (const auto& item : root["start_policy"].items()) {
          const auto& rule = item.value();
          if (!rule.is_object()) {
            remarks.emplace_back("start_policy." + item.key() + " must be an object");
            ok = false;
            continue;
          }
          if (!rule.value("enabled", true)) continue;
          out.auto_start = rule.value("auto_start", out.auto_start);
          out.repeat_until_window_end = rule.value("repeat_until_window_end", out.repeat_until_window_end);
          out.required_battery_state = rule.value("required_battery_state", out.required_battery_state);
          out.minimum_remaining_window_minutes = rule.value("minimum_remaining_window_minutes", out.minimum_remaining_window_minutes);
          break;
        }
      }
    }

    if (!isValidRequiredBatteryState(out.required_battery_state)) {
      remarks.emplace_back("start_policy required_battery_state has invalid value: " + out.required_battery_state);
      ok = false;
    }

    if (!root.contains("timetable") || !root["timetable"].is_object()) {
      remarks.emplace_back("timetable object is missing");
      ok = false;
    } else {
      for (const auto& item : root["timetable"].items()) {
        const std::string id = item.key();
        const auto& entry_json = item.value();
        if (!entry_json.is_object()) {
          remarks.emplace_back("timetable." + id + " must be an object");
          ok = false;
          continue;
        }

        TimetableEntry entry;
        entry.id = id;
        entry.day = entry_json.value("day", "");
        entry.start_text = entry_json.value("start", "");
        entry.end_text = entry_json.value("end", "");
        entry.end_behavior = entry_json.value("end_behavior", out.default_end_behavior);
        entry.enabled = entry_json.value("enabled", true);
        entry.auto_start = entry_json.value("auto_start", out.auto_start);
        entry.required_battery_state = entry_json.value("required_battery_state", out.required_battery_state);
        entry.minimum_remaining_window_minutes = entry_json.value("minimum_remaining_window_minutes",
                                                                  out.minimum_remaining_window_minutes);

        if (!isValidDay(entry.day)) {
          remarks.emplace_back("timetable." + id + ".day has invalid value: " + entry.day);
          ok = false;
        }
        if (!parseTimeHHMM(entry.start_text, entry.start_minutes)) {
          remarks.emplace_back("timetable." + id + ".start has invalid format, expected HH:MM");
          ok = false;
        }
        if (!parseTimeHHMM(entry.end_text, entry.end_minutes)) {
          remarks.emplace_back("timetable." + id + ".end has invalid format, expected HH:MM");
          ok = false;
        }
        if (entry.end_minutes <= entry.start_minutes) {
          remarks.emplace_back("timetable." + id + ".end must be after start for the first implementation");
          ok = false;
        }
        if (!isValidEndBehavior(entry.end_behavior)) {
          remarks.emplace_back("timetable." + id + ".end_behavior has invalid value: " + entry.end_behavior);
          ok = false;
        }
        if (!isValidRequiredBatteryState(entry.required_battery_state)) {
          remarks.emplace_back("timetable." + id + ".required_battery_state has invalid value: " +
                               entry.required_battery_state);
          ok = false;
        }
        if (entry.minimum_remaining_window_minutes < 0) {
          remarks.emplace_back("timetable." + id + ".minimum_remaining_window_minutes must be >= 0");
          ok = false;
        }

        out.entries.push_back(entry);
      }
    }

    if (out.entries.empty()) {
      remarks.emplace_back("timetable has no entries; service will never trigger missions");
    }

    if (ok) {
      remarks.emplace_back("Timetable loaded successfully");
    }
    return ok;
  }

  Evaluation evaluate() const {
    Evaluation e;
    e.time_source = "system";

    if (!valid_) {
      e.reason = "invalid_timetable";
      return e;
    }

    e.time_valid = !config_.require_valid_time || systemTimeLooksValid();
    if (!e.time_valid) {
      e.reason = "time_invalid";
      return e;
    }

    const std::time_t now = std::time(nullptr);
    std::tm local_tm{};
    localtime_r(&now, &local_tm);
    const std::string today = weekdayName(local_tm);
    const int now_minutes = local_tm.tm_hour * 60 + local_tm.tm_min;

    const TimetableEntry* active = nullptr;
    for (const auto& entry : config_.entries) {
      if (!entry.enabled) continue;
      if (entry.day != today) continue;
      if (now_minutes >= entry.start_minutes && now_minutes < entry.end_minutes) {
        active = &entry;
        break;
      }
    }

    if (active != nullptr) {
      e.active_window = true;
      e.start_allowed = true;
      e.active_entry_id = active->id;
      e.active_day = active->day;
      e.active_start = active->start_text;
      e.active_end = active->end_text;
      e.end_behavior = active->end_behavior;
      e.remaining_window_minutes = active->end_minutes - now_minutes;
      e.reason = "inside_active_window";
    } else {
      e.start_allowed = config_.outside_allow_start;
      e.reason = config_.outside_allow_start ? "outside_window_start_allowed_by_rule" : "outside_time_window";
    }

    e.mission_trigger_allowed = canTriggerMission(e);
    return e;
  }

  bool canTriggerMission(const Evaluation& e) const {
    if (!valid_) return false;
    if (!e.time_valid) return false;
    if (!e.active_window) return false;
    if (!config_.auto_start) return false;
    if (!has_high_level_status_) return false;
    if (last_high_level_status_.emergency) return false;
    if (last_high_level_status_.state != mower_msgs::HighLevelStatus::HIGH_LEVEL_STATE_IDLE) return false;
    if (!last_high_level_status_.is_charging) return false;
    if (mission_started_by_service_) return false;
    if (!config_.repeat_until_window_end && last_started_entry_id_ == e.active_entry_id) return false;

    int required_remaining = default_minimum_remaining_window_minutes_;
    std::string required_battery_state = config_.required_battery_state;
    for (const auto& entry : config_.entries) {
      if (entry.id == e.active_entry_id) {
        required_remaining = entry.minimum_remaining_window_minutes;
        required_battery_state = entry.required_battery_state;
        if (!entry.auto_start) return false;
        break;
      }
    }

    const double required_battery_threshold =
        required_battery_state == "sufficient" ? battery_sufficient_threshold_ : battery_full_threshold_;
    if (last_high_level_status_.battery_percent < required_battery_threshold) return false;
    if (e.remaining_window_minutes < required_remaining) return false;

    if (!last_start_command_time_.isZero() &&
        (ros::Time::now() - last_start_command_time_).toSec() < start_command_cooldown_seconds_) {
      return false;
    }

    return true;
  }

  void evaluateAndPublish() {
    Evaluation e = evaluate();

    if (valid_) {
      if (e.mission_trigger_allowed) {
        requestStart(e);
      }
      handleWindowEnd(e);
    }

    std_msgs::Bool start_allowed_msg;
    start_allowed_msg.data = e.start_allowed;
    start_allowed_pub_.publish(start_allowed_msg);

    std_msgs::Bool trigger_allowed_msg;
    trigger_allowed_msg.data = e.mission_trigger_allowed;
    trigger_allowed_pub_.publish(trigger_allowed_msg);

    publishStatus(buildStatusJson(valid_, remarks_, e, valid_ ? &config_.raw : nullptr));

    last_evaluation_ = e;
  }

  void requestStart(const Evaluation& e) {
    last_start_command_time_ = ros::Time::now();
    last_started_entry_id_ = e.active_entry_id;

    if (dry_run_) {
      mission_started_by_service_ = true;
      service_started_mission_left_dock_ = false;
      ROS_WARN_STREAM("DRY RUN: would request mission start for timetable entry " << e.active_entry_id);
      return;
    }

    mower_msgs::HighLevelControlSrv srv;
    srv.request.command = mower_msgs::HighLevelControlSrvRequest::COMMAND_START;
    if (high_level_client_.call(srv)) {
      mission_started_by_service_ = true;
      service_started_mission_left_dock_ = false;
      ROS_INFO_STREAM("Requested mission start for timetable entry " << e.active_entry_id);
    } else {
      ROS_ERROR_STREAM("Failed to call mower_service/high_level_control COMMAND_START");
    }
  }

  void handleWindowEnd(const Evaluation& e) {
    if (last_evaluation_.active_window && !e.active_window) {
      const std::string behavior = last_evaluation_.end_behavior.empty() ? config_.default_end_behavior : last_evaluation_.end_behavior;
      if (behavior == "return_to_dock") {
        if (has_high_level_status_ && last_high_level_status_.state == mower_msgs::HighLevelStatus::HIGH_LEVEL_STATE_AUTONOMOUS) {
          if (mission_started_by_service_ || control_manual_missions_on_window_end_) {
            requestHome("time window ended");
          }
        }
      } else if (behavior == "finish_current_run") {
        ROS_INFO_STREAM("Time window ended, configured to finish current run");
      } else if (behavior == "pause") {
        ROS_WARN_STREAM("Time window ended, pause behavior is not implemented yet");
      } else if (behavior == "stop") {
        ROS_WARN_STREAM("Time window ended, stop behavior is not implemented yet");
      }
    }

    if (last_evaluation_.active_window && !e.active_window) {
      last_started_entry_id_.clear();
    }

    if (mission_started_by_service_ && has_high_level_status_) {
      const bool idle_and_charging =
          last_high_level_status_.state == mower_msgs::HighLevelStatus::HIGH_LEVEL_STATE_IDLE && last_high_level_status_.is_charging;

      if (!idle_and_charging) {
        service_started_mission_left_dock_ = true;
      } else if (service_started_mission_left_dock_) {
        mission_started_by_service_ = false;
        service_started_mission_left_dock_ = false;
      }
    }
  }

  void requestHome(const std::string& reason) {
    if (dry_run_) {
      ROS_WARN_STREAM("DRY RUN: would request docking/home because " << reason);
      return;
    }
    mower_msgs::HighLevelControlSrv srv;
    srv.request.command = mower_msgs::HighLevelControlSrvRequest::COMMAND_HOME;
    if (high_level_client_.call(srv)) {
      ROS_INFO_STREAM("Requested docking/home because " << reason);
    } else {
      ROS_ERROR_STREAM("Failed to call mower_service/high_level_control COMMAND_HOME");
    }
  }

  json buildStatusJson(bool valid, const std::vector<std::string>& remarks, const Evaluation& e, const json* timetable) const {
    json status;
    status["valid"] = valid;
    status["remarks"] = remarks;
    status["time"] = {
        {"valid", e.time_valid},
        {"source", e.time_source},
        {"timezone", valid ? config_.timezone : nullptr},
    };
    status["state"] = {
        {"active_window", e.active_window},
        {"start_allowed", e.start_allowed},
        {"mission_trigger_allowed", e.mission_trigger_allowed},
        {"active_entry_id", e.active_entry_id.empty() ? json(nullptr) : json(e.active_entry_id)},
        {"active_day", e.active_day.empty() ? json(nullptr) : json(e.active_day)},
        {"active_start", e.active_start.empty() ? json(nullptr) : json(e.active_start)},
        {"active_end", e.active_end.empty() ? json(nullptr) : json(e.active_end)},
        {"remaining_window_minutes", e.remaining_window_minutes},
        {"end_behavior", e.end_behavior.empty() ? json(nullptr) : json(e.end_behavior)},
        {"reason", e.reason},
        {"mission_started_by_service", mission_started_by_service_},
        {"service_started_mission_left_dock", service_started_mission_left_dock_},
        {"last_started_entry_id", last_started_entry_id_.empty() ? json(nullptr) : json(last_started_entry_id_)},
        {"has_high_level_status", has_high_level_status_},
    };
    if (has_high_level_status_) {
      status["robot"] = {
          {"state", last_high_level_status_.state},
          {"state_name", last_high_level_status_.state_name},
          {"is_charging", last_high_level_status_.is_charging},
          {"battery_percent", last_high_level_status_.battery_percent},
          {"battery_full_threshold", battery_full_threshold_},
          {"battery_sufficient_threshold", battery_sufficient_threshold_},
          {"emergency", last_high_level_status_.emergency},
      };
    }
    status["timetable"] = timetable == nullptr ? json(nullptr) : *timetable;
    return status;
  }

  void publishStatus(const json& status) const {
    std_msgs::String msg;
    msg.data = status.dump(2);
    status_pub_.publish(msg);
  }

  ros::NodeHandle nh_;
  ros::NodeHandle private_nh_;
  ros::Publisher status_pub_;
  ros::Publisher start_allowed_pub_;
  ros::Publisher trigger_allowed_pub_;
  ros::Subscriber high_level_sub_;
  ros::ServiceClient high_level_client_;
  ros::ServiceServer reload_srv_;
  ros::Timer timer_;
  xbot_rpc::RpcProvider rpc_provider_;

  std::string timetable_file_;
  double check_rate_hz_ = 1.0;
  double battery_full_threshold_ = 0.98;
  double battery_sufficient_threshold_ = 0.30;
  int default_minimum_remaining_window_minutes_ = 30;
  double start_command_cooldown_seconds_ = 60.0;
  bool dry_run_ = false;
  bool control_manual_missions_on_window_end_ = false;

  TimetableConfig config_;
  bool valid_ = false;
  std::vector<std::string> remarks_;
  Evaluation last_evaluation_;

  mower_msgs::HighLevelStatus last_high_level_status_;
  bool has_high_level_status_ = false;
  bool mission_started_by_service_ = false;
  bool service_started_mission_left_dock_ = false;
  std::string last_started_entry_id_;
  ros::Time last_start_command_time_;
};

int main(int argc, char** argv) {
  ros::init(argc, argv, "timetable_service");
  TimetableService service;
  ros::spin();
  return 0;
}
