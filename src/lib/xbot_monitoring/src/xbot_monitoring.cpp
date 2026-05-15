//
// Created by Clemens Elflein on 22.11.22.
// Copyright (c) 2022 Clemens Elflein. All rights reserved.
//
#include <filesystem>
#include <algorithm>
#include <cstdio>
#include <fstream>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>

#include "ros/ros.h"
#include <memory>
#include <boost/regex.hpp>
#include "xbot_msgs/SensorInfo.h"
#include "xbot_msgs/SensorDataString.h"
#include "xbot_msgs/SensorDataDouble.h"
#include "xbot_msgs/RobotState.h"
#include <mqtt/async_client.h>
#include <nlohmann/json.hpp>
#include <vector>
#include <set>
#include "geometry_msgs/Twist.h"
#include "std_msgs/String.h"
#include "xbot_msgs/RegisterActionsSrv.h"
#include "xbot_msgs/ActionInfo.h"
#include "xbot_msgs/MapOverlay.h"
#include "xbot_rpc/RpcError.h"
#include "xbot_rpc/RpcRequest.h"
#include "xbot_rpc/RpcResponse.h"
#include "xbot_rpc/constants.h"
#include "xbot_rpc/provider.h"
#include "xbot_rpc/RegisterMethodsSrv.h"
#include "capabilities.h"

using json = nlohmann::ordered_json;

void publish_capabilities();
void publish_sensor_metadata();
void publish_map();
void publish_map_validation(const json &validation);
json validate_map_payload_for_mqtt(const json &payload);
void publish_map_overlay();
void publish_timetable();
void maybe_publish_timetable(bool force = false);
void publish_timetable_validation(const json &validation);
void publish_statustransition_log(std::size_t requested_limit = 0);
void publish_actions();
void publish_version();
void publish_params();
void rpc_request_callback(const std::string &payload);

// Stores registered actions (prefix to vector<action>)
std::map<std::string, std::vector<xbot_msgs::ActionInfo>> registered_actions;
std::mutex registered_actions_mutex;

// Stores registered RPC methods
std::map<std::string, std::vector<std::string>> registered_methods;
std::mutex registered_methods_mutex;

std::map<std::string, xbot_msgs::SensorInfo> found_sensors;
std::mutex found_sensors_mutex;

ros::NodeHandle *n;

// The MQTT Client
std::shared_ptr<mqtt::async_client> client_;
std::shared_ptr<mqtt::async_client> client_external_;


// Publisher for cmd_vel and commands
ros::Publisher cmd_vel_pub;
ros::Publisher action_pub;
ros::Publisher rpc_request_pub;

// properties for external mqtt
bool external_mqtt_enable = false;
std::string external_mqtt_username = "";
std::string external_mqtt_password = "";
std::string external_mqtt_hostname = "";
std::string external_mqtt_topic_prefix = "";
std::string external_mqtt_port = "";
std::string version_string = "";

class MqttCallback : public mqtt::callback {

    void connected(const mqtt::string &string) override {
        ROS_INFO_STREAM("MQTT Connected");
        publish_capabilities();
        publish_sensor_metadata();
        publish_map();
        publish_map_overlay();
        publish_timetable();
        publish_statustransition_log();
        publish_actions();
        publish_version();
        publish_params();

        // BEGIN: Deprecated code (1/2)
        // Earlier implementations subscribed to "/action" and "prefix//action" topics, we do it to not break stuff as well.
        client_->subscribe(this->mqtt_topic_prefix + "/teleop", 0);
        client_->subscribe(this->mqtt_topic_prefix + "/command", 0);
        client_->subscribe(this->mqtt_topic_prefix + "/action", 0);
        // END: Deprecated code (1/2)

        client_->subscribe(this->mqtt_topic_prefix + "teleop", 0);
        client_->subscribe(this->mqtt_topic_prefix + "command", 0);
        client_->subscribe(this->mqtt_topic_prefix + "action", 0);
        client_->subscribe(this->mqtt_topic_prefix + "rpc/request", 0);
        client_->subscribe(this->mqtt_topic_prefix + "timetable/set/json", 0);
        client_->subscribe(this->mqtt_topic_prefix + "timetable/set/bson", 0);
        client_->subscribe(this->mqtt_topic_prefix + "timetable/set/renew/json", 0);
        client_->subscribe(this->mqtt_topic_prefix + "timetable/set/renew/bson", 0);
        client_->subscribe(this->mqtt_topic_prefix + "timetable/set/suspension/json", 0);
        client_->subscribe(this->mqtt_topic_prefix + "timetable/set/suspension/bson", 0);
        client_->subscribe(this->mqtt_topic_prefix + "map/set/renew/json", 0);
        client_->subscribe(this->mqtt_topic_prefix + "map/set/json", 0);
        client_->subscribe(this->mqtt_topic_prefix + "statustransition_log/set/renew/json", 0);
    }

public:
    void setMqttClient(std::shared_ptr<mqtt::async_client> c, const std::string &mqtt_topic_prefix) {
        this->client_ = std::move(c);
        this->mqtt_topic_prefix = mqtt_topic_prefix;
    }
    void message_arrived(mqtt::const_message_ptr ptr) override {
        if(ptr->get_topic() == this->mqtt_topic_prefix + "teleop") {
            try {
                json json = json::from_bson(ptr->get_payload().begin(), ptr->get_payload().end());
                geometry_msgs::Twist t;
                t.linear.x = json["vx"];
                t.angular.z = json["vz"];
                cmd_vel_pub.publish(t);
            } catch (const json::exception &e) {
                ROS_ERROR_STREAM("Error decoding teleop bson: " << e.what());
            }
        } else if(ptr->get_topic() == this->mqtt_topic_prefix + "action") {
            ROS_INFO_STREAM("Got action: " + ptr->get_payload());
            std_msgs::String action_msg;
            action_msg.data = ptr->get_payload_str();
            action_pub.publish(action_msg);
        } else if(ptr->get_topic() == this->mqtt_topic_prefix + "/action") {
            // BEGIN: Deprecated code (2/2)
            ROS_WARN_STREAM("Got action on deprecated topic! Change your topic names!: " + ptr->get_payload());
            std_msgs::String action_msg;
            action_msg.data = ptr->get_payload_str();
            action_pub.publish(action_msg);
            // END: Deprecated code (2/2)
        } else if (ptr->get_topic() == this->mqtt_topic_prefix + "rpc/request") {
          std::string payload = ptr->get_payload_str();
          rpc_request_callback(payload);
        } else if (ptr->get_topic() == this->mqtt_topic_prefix + "timetable/set/json") {
            try {
                json payload = json::parse(ptr->get_payload_str());
                xbot_rpc::RpcRequest msg;
                msg.method = "timetable.replace";
                msg.params = json::array({payload}).dump();
                msg.id = "mqtt_timetable_set_json";
                rpc_request_pub.publish(msg);
            } catch (const json::exception &e) {
                publish_timetable_validation({{"valid", false}, {"remarks", {std::string("Error decoding timetable JSON: ") + e.what()}}});
            }
        } else if (ptr->get_topic() == this->mqtt_topic_prefix + "timetable/set/bson") {
            try {
                json payload = json::from_bson(ptr->get_payload().begin(), ptr->get_payload().end());
                if (payload.is_object() && payload.contains("d")) {
                    payload = payload["d"];
                }
                xbot_rpc::RpcRequest msg;
                msg.method = "timetable.replace";
                msg.params = json::array({payload}).dump();
                msg.id = "mqtt_timetable_set_bson";
                rpc_request_pub.publish(msg);
            } catch (const json::exception &e) {
                publish_timetable_validation({{"valid", false}, {"remarks", {std::string("Error decoding timetable BSON: ") + e.what()}}});
            }
        } else if (ptr->get_topic() == this->mqtt_topic_prefix + "timetable/set/suspension/json") {
            try {
                json payload = json::parse(ptr->get_payload_str());
                xbot_rpc::RpcRequest msg;
                msg.method = "timetable.suspension_set";
                msg.params = json::array({payload}).dump();
                msg.id = "mqtt_timetable_suspension_set_json";
                rpc_request_pub.publish(msg);
            } catch (const json::exception &e) {
                publish_timetable_validation({{"valid", false}, {"remarks", {std::string("Error decoding suspension JSON: ") + e.what()}}});
            }
        } else if (ptr->get_topic() == this->mqtt_topic_prefix + "timetable/set/suspension/bson") {
            try {
                json payload = json::from_bson(ptr->get_payload().begin(), ptr->get_payload().end());
                if (payload.is_object() && payload.contains("d")) {
                    payload = payload["d"];
                }
                xbot_rpc::RpcRequest msg;
                msg.method = "timetable.suspension_set";
                msg.params = json::array({payload}).dump();
                msg.id = "mqtt_timetable_suspension_set_bson";
                rpc_request_pub.publish(msg);
            } catch (const json::exception &e) {
                publish_timetable_validation({{"valid", false}, {"remarks", {std::string("Error decoding suspension BSON: ") + e.what()}}});
            }
        } else if (ptr->get_topic() == this->mqtt_topic_prefix + "map/set/json") {
            try {
                json payload = json::parse(ptr->get_payload_str());
                json validation = validate_map_payload_for_mqtt(payload);
                if (!validation.value("valid", false)) {
                    publish_map_validation(validation);
                } else {
                    xbot_rpc::RpcRequest msg;
                    msg.method = "map.replace";
                    msg.params = json::array({payload}).dump();
                    msg.id = "mqtt_map_set_json";
                    rpc_request_pub.publish(msg);
                }
            } catch (const json::exception &e) {
                publish_map_validation({{"valid", false}, {"remarks", {std::string("Error decoding map JSON: ") + e.what()}}});
            }
        } else if (ptr->get_topic() == this->mqtt_topic_prefix + "map/set/renew/json") {
            // App opened the areas page and requests the current retained map again.
            // The payload is intentionally optional; any message on this topic triggers a republish.
            publish_map();
        } else if (ptr->get_topic() == this->mqtt_topic_prefix + "statustransition_log/set/renew/json") {
            // App requests the current retained status transition log again.
            // Empty payload: use the configured default limit.
            // Optional JSON payload: {"limit": 50} returns the newest N entries.
            std::size_t requested_limit = 0;
            const std::string payload_text = ptr->get_payload_str();
            if (!payload_text.empty()) {
                try {
                    json payload = json::parse(payload_text);
                    if (payload.is_object() && payload.contains("limit") && payload["limit"].is_number_unsigned()) {
                        requested_limit = payload["limit"].get<std::size_t>();
                    } else if (payload.is_object() && payload.contains("limit") && payload["limit"].is_number_integer()) {
                        const auto limit = payload["limit"].get<long long>();
                        if (limit > 0) {
                            requested_limit = static_cast<std::size_t>(limit);
                        }
                    }
                } catch (const json::exception &e) {
                    ROS_WARN_STREAM("Error decoding statustransition log renew JSON: " << e.what()
                                    << ". Falling back to configured default limit.");
                }
            }
            publish_statustransition_log(requested_limit);
        } else if (ptr->get_topic() == this->mqtt_topic_prefix + "timetable/set/renew/json" ||
                   ptr->get_topic() == this->mqtt_topic_prefix + "timetable/set/renew/bson") {
            // App opened the timetable page and requests the current retained timetable again.
            // The payload is intentionally optional; any message on this topic triggers a republish.
            maybe_publish_timetable(true);
        }
    }
private:
    std::shared_ptr<mqtt::async_client> client_;
    std::string mqtt_topic_prefix = "";
};

MqttCallback mqtt_callback;
MqttCallback mqtt_callback_external;

json map;
std::mutex map_mutex;
json map_overlay;
std::mutex map_overlay_mutex;
bool has_map = false;
bool has_map_overlay = false;

json timetable_status = json::object();
json timetable_confirmed = json::object();
std::mutex timetable_mutex;
bool has_timetable = false;
bool timetable_auto_mowing_time = false;
std::string timetable_auto_mow_id = "";
json timetable_auto_mow_suspension = 0;
ros::Time last_timetable_publish_time;
double mqtt_timetable_publish_interval_sec = 60.0;

std::string statustransition_log_file = "/data/ros/log_statustransition.json";
constexpr std::size_t STATUSTRANSITION_LOG_MAX_ENTRIES = 300;
std::size_t mqtt_statustransition_log_default_limit = 20;
std::mutex statustransition_log_mutex;
json statustransition_log_entries = json::array();
bool statustransition_log_loaded = false;
bool has_last_statustransition_key = false;
std::string last_statustransition_state;
std::string last_statustransition_sub_state;
bool last_statustransition_charging = false;
bool last_statustransition_emergency = false;
bool has_last_statustransition_timestamp = false;
std::chrono::system_clock::time_point last_statustransition_timestamp;

std::mutex latest_double_sensor_values_mutex;
std::map<std::string, double> latest_double_sensor_values;

xbot_rpc::RpcProvider rpc_provider("xbot_monitoring", {{
    RPC_METHOD("rpc.ping", {
        return "pong";
    }),
    RPC_METHOD("rpc.methods", {
        std::lock_guard<std::mutex> lk(registered_methods_mutex);
        json methods = json::array();
        for (const auto& [_, method_ids] : registered_methods) {
            for (const auto& method_id : method_ids) {
                methods.push_back(method_id);
            }
        }
        std::sort(methods.begin(), methods.end());
        return methods;
    }),
}});

void setupMqttClient() {
    // setup mqtt client for app use
    {
        // MQTT connection options
        mqtt::connect_options connect_options_;

        // basic client connection options
        connect_options_.set_automatic_reconnect(true);
        connect_options_.set_clean_session(true);
        connect_options_.set_keep_alive_interval(1000);
        connect_options_.set_max_inflight(10);

        // create MQTT client
        std::string uri = "tcp" + std::string("://") + "127.0.0.1" +
                          std::string(":") + std::to_string(1883);

        try {
            client_ = std::make_shared<mqtt::async_client>(
                    uri, "xbot_monitoring");
            mqtt_callback.setMqttClient(client_, "");
            client_->set_callback(mqtt_callback);

            client_->connect(connect_options_);

        } catch (const mqtt::exception &e) {
            ROS_ERROR("Client could not be initialized: %s", e.what());
            exit(EXIT_FAILURE);
        }
    }
    // setup external mqtt client
    if(external_mqtt_enable) {
        // MQTT connection options
        mqtt::connect_options connect_options_;

        // basic client connection options
        connect_options_.set_automatic_reconnect(true);
        connect_options_.set_clean_session(true);
        connect_options_.set_keep_alive_interval(1000);
        connect_options_.set_max_inflight(10);

        if(!external_mqtt_username.empty()) {
            connect_options_.set_user_name(external_mqtt_username);
            connect_options_.set_password(external_mqtt_password);
        }

        // create MQTT client
        std::string uri = "tcp" + std::string("://") + external_mqtt_hostname +
                          std::string(":") + external_mqtt_port;

        try {
            client_external_ = std::make_shared<mqtt::async_client>(
                    uri, "ext_xbot_monitoring");
            mqtt_callback_external.setMqttClient(client_external_, external_mqtt_topic_prefix);
            client_external_->set_callback(mqtt_callback_external);

            client_external_->connect(connect_options_);

        } catch (const mqtt::exception &e) {
            ROS_ERROR("External Client could not be initialized: %s", e.what());
            exit(EXIT_FAILURE);
        }
    }
}

void try_publish(std::string topic, std::string data, bool retain = false) {
    try {
        if (retain) {
            // QOS 1 so that the data actually arrives at the client at least once.
            client_->publish(topic, data, 1, true);
        } else {
            client_->publish(topic, data);
        }
    } catch (const mqtt::exception &e) {
        // client disconnected or something, we drop it.
    }
    // publish external
    if(external_mqtt_enable) {
        try {
            if (retain) {
                // QOS 1 so that the data actually arrives at the client at least once.
                client_external_->publish(external_mqtt_topic_prefix + topic, data, 1, true);
            } else {
                client_external_->publish(external_mqtt_topic_prefix + topic, data);
            }
        } catch (const mqtt::exception &e) {
            // client disconnected or something, we drop it.
        }
    }
}

void try_publish_binary(std::string topic, const void *data, size_t size, bool retain = false) {
    try {
        if (retain) {
            // QOS 1 so that the data actually arrives at the client at least once.
            client_->publish(topic, data, size, 1, true);
        } else {
            client_->publish(topic, data, size);
        }
    } catch (const mqtt::exception &e) {
        // client disconnected or something, we drop it.
    }
}

void publish_version() {
    json version = {
            {"version", version_string}
    };
    try_publish("version/json", version.dump(), true);
    auto bson = json::to_bson(version);
    try_publish_binary("version", bson.data(), bson.size(), true);
}

void publish_capabilities() {
  try_publish("capabilities/json", CAPABILITIES.dump(2), true);
}

#pragma GCC diagnostic push
#pragma GCC diagnostic warning "-Wswitch-enum"
json xmlrpc_to_json(XmlRpc::XmlRpcValue value) {
    switch (value.getType()) {
        case XmlRpc::XmlRpcValue::TypeBoolean:
            return static_cast<bool>(value);
        case XmlRpc::XmlRpcValue::TypeInt:
            return static_cast<int>(value);
        case XmlRpc::XmlRpcValue::TypeDouble:
            return static_cast<double>(value);
        case XmlRpc::XmlRpcValue::TypeString:
            return static_cast<std::string>(value);
        case XmlRpc::XmlRpcValue::TypeArray: {
            json arr = json::array();
            for (int i = 0; i < value.size(); ++i)
                arr.push_back(xmlrpc_to_json(value[i]));
            return arr;
        }
        case XmlRpc::XmlRpcValue::TypeStruct: {
            json obj = json::object();
            for (auto it = value.begin(); it != value.end(); ++it)
                obj[it->first] = xmlrpc_to_json(it->second);
            return obj;
        }
        case XmlRpc::XmlRpcValue::TypeDateTime: {
            const struct tm& t = static_cast<const struct tm&>(value);
            char buf[32];
            std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", &t);
            return std::string(buf);
        }
        case XmlRpc::XmlRpcValue::TypeBase64: {
            const XmlRpc::XmlRpcValue::BinaryData& data = static_cast<const XmlRpc::XmlRpcValue::BinaryData&>(value);
            static const char* b64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
            std::string out;
            out.reserve(((data.size() + 2) / 3) * 4);
            for (size_t i = 0; i < data.size(); i += 3) {
                unsigned int n = (static_cast<unsigned char>(data[i]) << 16)
                    | (i + 1 < data.size() ? static_cast<unsigned char>(data[i + 1]) << 8 : 0)
                    | (i + 2 < data.size() ? static_cast<unsigned char>(data[i + 2]) : 0);
                out += b64[(n >> 18) & 0x3F];
                out += b64[(n >> 12) & 0x3F];
                out += (i + 1 < data.size()) ? b64[(n >> 6) & 0x3F] : '=';
                out += (i + 2 < data.size()) ? b64[n & 0x3F] : '=';
            }
            return out;
        }
        case XmlRpc::XmlRpcValue::TypeInvalid:
            return nullptr;
    }
    return nullptr;
}
#pragma GCC diagnostic pop

void publish_params() {
    std::vector<std::string> param_names;
    ros::param::getParamNames(param_names);
    std::sort(param_names.begin(), param_names.end());

    json params = json::object();
    for (const auto &name : param_names) {
        if (name.find("password") != std::string::npos) {
            params[name] = nullptr;
            continue;
        }
        XmlRpc::XmlRpcValue value;
        if (ros::param::get(name, value)) {
            params[name] = xmlrpc_to_json(value);
        }
    }
    try_publish("params/json", params.dump(), true);
}

void publish_sensor_metadata() {
    json sensor_info;
    {
        std::unique_lock<std::mutex> lk(found_sensors_mutex);

        if(found_sensors.empty())
            return;

        for (const auto &kv: found_sensors) {
            json info;
            info["sensor_id"] = kv.second.sensor_id;
            info["sensor_name"] = kv.second.sensor_name;

            switch (kv.second.value_type) {
                case xbot_msgs::SensorInfo::TYPE_STRING: {
                    info["value_type"] = "STRING";
                    break;
                }
                case xbot_msgs::SensorInfo::TYPE_DOUBLE: {
                    info["value_type"] = "DOUBLE";
                    break;
                }
                default: {
                    info["value_type"] = "UNKNOWN";
                    break;
                }


            }

            switch (kv.second.value_description) {
                case xbot_msgs::SensorInfo::VALUE_DESCRIPTION_TEMPERATURE: {
                    info["value_description"] = "TEMPERATURE";
                    break;
                }
                case xbot_msgs::SensorInfo::VALUE_DESCRIPTION_VELOCITY: {
                    info["value_description"] = "VELOCITY";
                    break;
                }
                case xbot_msgs::SensorInfo::VALUE_DESCRIPTION_ACCELERATION: {
                    info["value_description"] = "ACCELERATION";
                    break;
                }
                case xbot_msgs::SensorInfo::VALUE_DESCRIPTION_VOLTAGE: {
                    info["value_description"] = "VOLTAGE";
                    break;
                }
                case xbot_msgs::SensorInfo::VALUE_DESCRIPTION_CURRENT: {
                    info["value_description"] = "CURRENT";
                    break;
                }
                case xbot_msgs::SensorInfo::VALUE_DESCRIPTION_PERCENT: {
                    info["value_description"] = "PERCENT";
                    break;
                }
                case xbot_msgs::SensorInfo::VALUE_DESCRIPTION_RPM: {
                    info["value_description"] = "REVOLUTIONS";
                    break;
                }
                default: {
                    info["value_description"] = "UNKNOWN";
                    break;
                }
            }

            info["unit"] = kv.second.unit;
            info["has_min_max"] = kv.second.has_min_max;
            info["min_value"] = kv.second.min_value;
            info["max_value"] = kv.second.max_value;
            info["has_critical_low"] = kv.second.has_critical_low;
            info["lower_critical_value"] = kv.second.lower_critical_value;
            info["has_critical_high"] = kv.second.has_critical_high;
            info["upper_critical_value"] = kv.second.upper_critical_value;
            sensor_info.push_back(info);
        }
    }
    try_publish("sensor_infos/json", sensor_info.dump(), true);
    json data;
    data["d"] = sensor_info;
    auto bson = json::to_bson(data);
    try_publish_binary("sensor_infos/bson", bson.data(), bson.size(), true);
}

void subscribe_to_sensor(std::string topic, std::vector<ros::Subscriber> &sensor_data_subscribers) {
    xbot_msgs::SensorInfo sensor;
    {
        std::unique_lock<std::mutex> lk(found_sensors_mutex);
        sensor = found_sensors[topic];
    }

    ROS_INFO_STREAM("Subscribing to sensor data for sensor with name: " << sensor.sensor_name);

    std::string data_topic = "xbot_monitoring/sensors/" + sensor.sensor_id + "/data";

    switch (sensor.value_type) {
        case xbot_msgs::SensorInfo::TYPE_DOUBLE: {
            ros::Subscriber s = n->subscribe<xbot_msgs::SensorDataDouble>(data_topic, 10, [info = sensor](
                    const xbot_msgs::SensorDataDouble::ConstPtr &msg) {
                try_publish("sensors/" + info.sensor_id + "/data", std::to_string(msg->data));
                {
                    std::lock_guard<std::mutex> lk(latest_double_sensor_values_mutex);
                    latest_double_sensor_values[info.sensor_id] = msg->data;
                }

                json data;
                data["d"] = msg->data;
                auto bson = json::to_bson(data);
                try_publish_binary("sensors/" + info.sensor_id + "/bson", bson.data(), bson.size());
            });
            sensor_data_subscribers.push_back(s);
            break;
        }
        case xbot_msgs::SensorInfo::TYPE_STRING: {
            ros::Subscriber s = n->subscribe<xbot_msgs::SensorDataString>(data_topic, 10, [info = sensor](
                    const xbot_msgs::SensorDataString::ConstPtr &msg) {
                try_publish("sensors/" + info.sensor_id + "/data", msg->data);

                json data;
                data["d"] = msg->data;
                auto bson = json::to_bson(data);
                try_publish_binary("sensors/" + info.sensor_id + "/bson", bson.data(), bson.size());
            });
            sensor_data_subscribers.push_back(s);
            break;
        }
        default: {
            ROS_ERROR_STREAM("Invalid Sensor Data Type: " << (int) sensor.value_type);
        }
    }
}

std::string utc_timestamp_iso8601(const std::chrono::system_clock::time_point &time_point) {
    const auto time = std::chrono::system_clock::to_time_t(time_point);
    const auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(
        time_point.time_since_epoch()) % 1000;
    std::tm tm_utc{};
#if defined(_WIN32)
    gmtime_s(&tm_utc, &time);
#else
    gmtime_r(&time, &tm_utc);
#endif
    std::ostringstream out;
    out << std::put_time(&tm_utc, "%Y-%m-%dT%H:%M:%S")
        << '.' << std::setfill('0') << std::setw(3) << milliseconds.count() << 'Z';
    return out.str();
}

bool try_parse_utc_timestamp_iso8601(const std::string &timestamp,
                                     std::chrono::system_clock::time_point &time_point) {
    if (timestamp.size() < 20) {
        return false;
    }

    std::tm tm_utc{};
    std::istringstream date_part(timestamp.substr(0, 19));
    date_part >> std::get_time(&tm_utc, "%Y-%m-%dT%H:%M:%S");
    if (date_part.fail()) {
        return false;
    }

    long milliseconds = 0;
    if (timestamp.size() >= 24 && timestamp[19] == '.') {
        const std::string milliseconds_part = timestamp.substr(20, 3);
        if (milliseconds_part.find_first_not_of("0123456789") != std::string::npos) {
            return false;
        }
        milliseconds = std::stol(milliseconds_part);
    }

#if defined(_WIN32)
    const std::time_t seconds_since_epoch = _mkgmtime(&tm_utc);
#else
    const std::time_t seconds_since_epoch = timegm(&tm_utc);
#endif
    if (seconds_since_epoch == static_cast<std::time_t>(-1)) {
        return false;
    }

    time_point = std::chrono::system_clock::from_time_t(seconds_since_epoch) +
                 std::chrono::milliseconds(milliseconds);
    return true;
}

void load_statustransition_log_if_needed_locked() {
    if (statustransition_log_loaded) {
        return;
    }
    statustransition_log_loaded = true;
    statustransition_log_entries = json::array();

    try {
        std::ifstream in(statustransition_log_file);
        if (!in.good()) {
            return;
        }
        json existing = json::parse(in, nullptr, false);
        if (existing.is_object() && existing.contains("entries") && existing["entries"].is_array()) {
            statustransition_log_entries = existing["entries"];
        } else if (existing.is_array()) {
            statustransition_log_entries = existing;
        }
        while (statustransition_log_entries.size() > STATUSTRANSITION_LOG_MAX_ENTRIES) {
            statustransition_log_entries.erase(statustransition_log_entries.begin());
        }
        if (!statustransition_log_entries.empty()) {
            const auto &latest_entry = statustransition_log_entries.back();
            if (latest_entry.contains("timestamp") && latest_entry["timestamp"].is_string()) {
                has_last_statustransition_timestamp = try_parse_utc_timestamp_iso8601(
                    latest_entry["timestamp"].get<std::string>(),
                    last_statustransition_timestamp);
            }
        }
    } catch (const std::exception &e) {
        ROS_WARN_STREAM("Unable to load statustransition log '" << statustransition_log_file
                        << "': " << e.what());
        statustransition_log_entries = json::array();
    }
}

void persist_statustransition_log_locked() {
    try {
        const std::filesystem::path log_path(statustransition_log_file);
        const auto parent = log_path.parent_path();
        if (!parent.empty()) {
            std::filesystem::create_directories(parent);
        }
        const auto temp_path = log_path.string() + ".tmp";
        json root;
        root["max_entries"] = STATUSTRANSITION_LOG_MAX_ENTRIES;
        root["entries"] = statustransition_log_entries;
        {
            std::ofstream out(temp_path, std::ios::trunc);
            if (!out.good()) {
                ROS_WARN_STREAM("Unable to write statustransition log temp file '" << temp_path << "'.");
                return;
            }
            out << root.dump(2) << std::endl;
        }
        std::filesystem::rename(temp_path, log_path);
    } catch (const std::exception &e) {
        ROS_WARN_STREAM("Unable to persist statustransition log '" << statustransition_log_file
                        << "': " << e.what());
    }
}

std::size_t normalize_statustransition_log_limit(std::size_t requested_limit, std::size_t available_entries) {
    if (available_entries == 0) {
        return 0;
    }

    std::size_t effective_limit = requested_limit;
    if (effective_limit == 0) {
        effective_limit = mqtt_statustransition_log_default_limit;
    }
    if (effective_limit == 0) {
        effective_limit = available_entries;
    }

    effective_limit = std::min(effective_limit, STATUSTRANSITION_LOG_MAX_ENTRIES);
    effective_limit = std::min(effective_limit, available_entries);
    return effective_limit;
}

void publish_statustransition_log(std::size_t requested_limit) {
    json payload = json::object();
    {
        std::lock_guard<std::mutex> lk(statustransition_log_mutex);
        load_statustransition_log_if_needed_locked();

        json snapshot = statustransition_log_entries;
        if (!snapshot.empty() && has_last_statustransition_timestamp) {
            const auto current_timestamp = std::chrono::system_clock::now();
            const auto active_status_duration = std::chrono::duration<double>(
                current_timestamp - last_statustransition_timestamp).count();
            snapshot.back()["duration_seconds"] = std::max(0.0, active_status_duration);
            snapshot.back()["duration_is_current"] = true;
        }

        const std::size_t total_entries = snapshot.size();
        const std::size_t effective_limit = normalize_statustransition_log_limit(requested_limit, total_entries);
        json selected_entries = json::array();
        if (effective_limit > 0) {
            const std::size_t first_index = total_entries - effective_limit;
            for (std::size_t index = first_index; index < total_entries; ++index) {
                selected_entries.push_back(snapshot[index]);
            }
        }

        payload["total_entries"] = total_entries;
        payload["returned_entries"] = selected_entries.size();
        payload["limit"] = effective_limit;
        payload["entries"] = std::move(selected_entries);
    }

    // Retained resource payload for status log pages and external consumers.
    try_publish("statustransition_log/json", payload.dump(2), true);
}

json current_temperature_snapshot() {
    json temperatures = json::object();
    const std::vector<std::string> sensor_ids = {
        "om_left_esc_temp",
        "om_right_esc_temp",
        "om_mow_esc_temp",
        "om_mow_motor_temp"
    };
    std::lock_guard<std::mutex> lk(latest_double_sensor_values_mutex);
    for (const auto &sensor_id : sensor_ids) {
        auto it = latest_double_sensor_values.find(sensor_id);
        if (it != latest_double_sensor_values.end()) {
            temperatures[sensor_id] = it->second;
        } else {
            temperatures[sensor_id] = nullptr;
        }
    }
    return temperatures;
}

void maybe_append_statustransition_log(const xbot_msgs::RobotState::ConstPtr &msg) {
    std::lock_guard<std::mutex> lk(statustransition_log_mutex);
    load_statustransition_log_if_needed_locked();

    const bool is_transition = !has_last_statustransition_key ||
                               last_statustransition_state != msg->current_state ||
                               last_statustransition_sub_state != msg->current_sub_state ||
                               last_statustransition_charging != msg->is_charging ||
                               last_statustransition_emergency != msg->emergency;

    if (!is_transition) {
        return;
    }

    const auto transition_timestamp = std::chrono::system_clock::now();
    if (!statustransition_log_entries.empty() && has_last_statustransition_timestamp) {
        const auto previous_status_duration = std::chrono::duration<double>(
            transition_timestamp - last_statustransition_timestamp).count();
        statustransition_log_entries.back()["duration_seconds"] =
            std::max(0.0, previous_status_duration);
    }

    json entry;
    entry["timestamp"] = utc_timestamp_iso8601(transition_timestamp);
    entry["duration_seconds"] = 0.0;
    entry["state"] = msg->current_state;
    entry["sub_state"] = msg->current_sub_state;
    entry["previous_state"] = has_last_statustransition_key ? json(last_statustransition_state) : json(nullptr);
    entry["previous_sub_state"] = has_last_statustransition_key ? json(last_statustransition_sub_state) : json(nullptr);
    entry["battery_percentage"] = msg->battery_percentage;
    entry["gps_percentage"] = msg->gps_percentage;
    entry["is_charging"] = msg->is_charging;
    entry["emergency"] = msg->emergency;
    entry["position"]["x"] = msg->robot_pose.pose.pose.position.x;
    entry["position"]["y"] = msg->robot_pose.pose.pose.position.y;
    entry["position"]["heading"] = msg->robot_pose.vehicle_heading;
    entry["position"]["pos_accuracy"] = msg->robot_pose.position_accuracy;
    entry["position"]["heading_accuracy"] = msg->robot_pose.orientation_accuracy;
    entry["position"]["heading_valid"] = msg->robot_pose.orientation_valid;
    entry["temperatures"] = current_temperature_snapshot();

    statustransition_log_entries.push_back(entry);
    while (statustransition_log_entries.size() > STATUSTRANSITION_LOG_MAX_ENTRIES) {
        statustransition_log_entries.erase(statustransition_log_entries.begin());
    }
    persist_statustransition_log_locked();

    last_statustransition_state = msg->current_state;
    last_statustransition_sub_state = msg->current_sub_state;
    last_statustransition_charging = msg->is_charging;
    last_statustransition_emergency = msg->emergency;
    last_statustransition_timestamp = transition_timestamp;
    has_last_statustransition_timestamp = true;
    has_last_statustransition_key = true;
}

void robot_state_callback(const xbot_msgs::RobotState::ConstPtr &msg) {
    // Build a JSON and publish it
    json j;

    j["battery_percentage"] = msg->battery_percentage;
    j["gps_percentage"] = msg->gps_percentage;
    j["current_action_progress"] = msg->current_action_progress;
    j["current_state"] = msg->current_state;
    j["current_sub_state"] = msg->current_sub_state;
    j["current_area"] = msg->current_area;
    j["current_area_id"] = msg->current_area_id;
    j["current_path"] = msg->current_path;
    j["current_path_index"] = msg->current_path_index;
    j["emergency"] = msg->emergency;
    j["is_charging"] = msg->is_charging;
    {
        std::lock_guard<std::mutex> lk(timetable_mutex);
        j["AutoMow"] = timetable_auto_mowing_time ? 1 : 0;
        j["AutoMowID"] = timetable_auto_mow_id;
        j["AutoMowSuspension"] = timetable_auto_mow_suspension;
    }
    j["rain_detected"] = msg->rain_detected;
    j["pose"]["x"] = msg->robot_pose.pose.pose.position.x;
    j["pose"]["y"] = msg->robot_pose.pose.pose.position.y;
    j["pose"]["heading"] = msg->robot_pose.vehicle_heading;
    j["pose"]["pos_accuracy"] = msg->robot_pose.position_accuracy;
    j["pose"]["heading_accuracy"] = msg->robot_pose.orientation_accuracy;
    j["pose"]["heading_valid"] = msg->robot_pose.orientation_valid;

    maybe_append_statustransition_log(msg);

    try_publish("robot_state/json", j.dump());
    json data;
    data["d"] = j;
    auto bson = json::to_bson(data);
    try_publish_binary("robot_state/bson", bson.data(), bson.size());
}

void publish_actions() {
    json actions = json::array();
    {
        std::lock_guard<std::mutex> lk(registered_actions_mutex);
        for(const auto &kv : registered_actions) {
            for(const auto &action : kv.second) {
                json action_info;
                action_info["action_id"] = kv.first + "/" + action.action_id;
                action_info["action_name"] = action.action_name;
                action_info["enabled"] = action.enabled;
                actions.push_back(action_info);
            }
        }
    }

    try_publish("actions/json", actions.dump(), true);
    json data;
    data["d"] = actions;

    auto bson = json::to_bson(data);
    try_publish_binary("actions/bson", bson.data(), bson.size(), true);
}

void publish_map() {
    json m;
    {
        std::lock_guard<std::mutex> lk(map_mutex);
        if(!has_map)
            return;
        m = map;
    }
    try_publish("map/json", m.dump(2), true);
    json data;
    data["d"] = m;
    auto bson = json::to_bson(data);
    try_publish_binary("map/bson", bson.data(), bson.size(), true);
}

void publish_map_validation(const json &validation) {
    try_publish("map/validation/json", validation.dump(2), true);
}

json validate_map_payload_for_mqtt(const json &payload) {
    json remarks = json::array();

    if (!payload.is_object()) {
        return {{"valid", false}, {"remarks", {"Map payload must be a JSON object"}}};
    }

    if (!payload.contains("areas") || !payload["areas"].is_array()) {
        return {{"valid", false}, {"remarks", {"Map payload must contain an areas array"}}};
    }

    std::set<int> used_orders;
    for (const auto &area : payload["areas"]) {
        if (!area.is_object()) {
            remarks.push_back("Each area must be a JSON object");
            continue;
        }

        const std::string area_id = area.value("id", std::string("<unknown>"));
        json properties = area.value("properties", json::object());
        const std::string type = properties.value("type", std::string("draft"));

        if (type != "mow") {
            continue;
        }

        if (!properties.contains("mowing_order") || !properties["mowing_order"].is_number_integer()) {
            remarks.push_back("Mowing area " + area_id + " has missing or non-integer mowing_order");
            continue;
        }

        const int order = properties["mowing_order"].get<int>();
        if (order < 1 || order > 99) {
            remarks.push_back("Mowing area " + area_id + " has invalid mowing_order " + std::to_string(order) + " (allowed: 1-99)");
            continue;
        }

        if (!used_orders.insert(order).second) {
            char buf[3];
            std::snprintf(buf, sizeof(buf), "%02d", order);
            remarks.push_back(std::string("Mowing order ") + buf + " is used more than once");
        }
    }

    if (!remarks.empty()) {
        return {{"valid", false}, {"remarks", remarks}};
    }

    return {{"valid", true}, {"remarks", {"Map payload accepted"}}};
}

void publish_map_overlay() {
    json m;
    {
        std::lock_guard<std::mutex> lk(map_overlay_mutex);
        if(!has_map_overlay)
            return;
        m = map_overlay;
    }
    try_publish("map_overlay/json", m.dump(), true);
    json data;
    data["d"] = m;
    auto bson = json::to_bson(data);
    try_publish_binary("map_overlay/bson", bson.data(), bson.size(), true);
}

void publish_timetable_validation(const json &validation) {
    try_publish("timetable/validation/json", validation.dump(2), true);
    json data;
    data["d"] = validation;
    auto bson = json::to_bson(data);
    try_publish_binary("timetable/validation/bson", bson.data(), bson.size(), true);
}

void publish_timetable() {
    json confirmed;
    {
        std::lock_guard<std::mutex> lk(timetable_mutex);
        if(!has_timetable)
            return;
        confirmed = timetable_confirmed;
        last_timetable_publish_time = ros::Time::now();
    }

    // Confirmed timetable payload: same values as timetable.json / incoming MQTT payload.
    // This is the resource state and is retained. It is sent on boot, renew, reload/change,
    // and optionally in a slow heartbeat. There is intentionally no timetable_state topic.
    try_publish("timetable/json", confirmed.dump(2), true);
    json timetable_data;
    timetable_data["d"] = confirmed;
    auto timetable_bson = json::to_bson(timetable_data);
    try_publish_binary("timetable/bson", timetable_bson.data(), timetable_bson.size(), true);
}

void maybe_publish_timetable(bool force) {
    if (force) {
        publish_timetable();
        return;
    }

    if (mqtt_timetable_publish_interval_sec <= 0.0) {
        return;
    }

    if (last_timetable_publish_time.isZero() ||
        (ros::Time::now() - last_timetable_publish_time).toSec() >= mqtt_timetable_publish_interval_sec) {
        publish_timetable();
    }
}

void timetable_status_callback(const std_msgs::String::ConstPtr &msg) {
    try {
        json status = json::parse(msg->data);
        json confirmed = json::object();
        bool auto_mowing_time = false;
        std::string auto_mow_id;
        json auto_mow_suspension = 0;
        bool timetable_changed = false;

        if (status.is_object() && status.contains("timetable") && !status["timetable"].is_null()) {
            confirmed = status["timetable"];
        }

        if (status.is_object()) {
            json robot_state = json::object();
            if (status.contains("robot_state") && status["robot_state"].is_object()) {
                robot_state = status["robot_state"];
            } else if (status.contains("state") && status["state"].is_object()) {
                // Backwards compatibility with older timetable_service status field name.
                robot_state = status["state"];
            }

            if (robot_state.is_object()) {
                if (robot_state.contains("AutoMow")) {
                    auto_mowing_time = robot_state.value("AutoMow", 0) == 1;
                } else {
                    auto_mowing_time = robot_state.value("auto_mowing_time", false);
                }
                if (robot_state.contains("AutoMowID") && robot_state["AutoMowID"].is_string()) {
                    auto_mow_id = robot_state["AutoMowID"].get<std::string>();
                } else if (robot_state.contains("active_entry_id") && robot_state["active_entry_id"].is_string()) {
                    auto_mow_id = robot_state["active_entry_id"].get<std::string>();
                }
                if (robot_state.contains("AutoMowSuspension")) {
                    auto_mow_suspension = robot_state["AutoMowSuspension"];
                } else if (robot_state.contains("suspended_until") && robot_state["suspended_until"].is_string()) {
                    auto_mow_suspension = robot_state["suspended_until"];
                }
            }
        }

        if (!auto_mowing_time) {
            auto_mow_id.clear();
        }

        {
            std::lock_guard<std::mutex> lk(timetable_mutex);
            timetable_changed = !has_timetable || timetable_confirmed != confirmed;
            timetable_status = status;
            timetable_confirmed = confirmed;
            timetable_auto_mowing_time = auto_mowing_time;
            timetable_auto_mow_id = auto_mow_id;
            timetable_auto_mow_suspension = auto_mow_suspension;
            has_timetable = true;
        }

        // Publish the timetable resource only when it appears/changes. A slow heartbeat is
        // handled in the main loop, and app requests use timetable/set/renew/json or /bson.
        if (timetable_changed) {
            publish_timetable();
        }
    } catch (const json::exception &e) {
        ROS_ERROR_STREAM("Error processing timetable status JSON: " << e.what());
    }
}

void map_callback(const std_msgs::String::ConstPtr &msg) {
    try {
        json m = json::parse(msg->data);
        {
            std::lock_guard<std::mutex> lk(map_mutex);
            map = m;
            has_map = true;
        }
        publish_map();
    } catch (const json::exception &e) {
        ROS_ERROR_STREAM("Error processing map JSON: " << e.what());
    }
}


void map_overlay_callback(const xbot_msgs::MapOverlay::ConstPtr &msg) {
    // Build a JSON and publish it

    json polys;
    for(const auto &poly : msg->polygons) {
        if(poly.polygon.points.size() < 2)
            continue;
        json poly_j;
        {
            json outline_poly_j;
            for (const auto &pt: poly.polygon.points) {
                json p_j;
                p_j["x"] = pt.x;
                p_j["y"] = pt.y;
                outline_poly_j.push_back(p_j);
            }
            poly_j["poly"] = outline_poly_j;
            poly_j["is_closed"] = poly.closed;
            poly_j["line_width"] = poly.line_width;
            poly_j["color"] = poly.color;
        }
        polys.push_back(poly_j);
    }

    json j;
    j["polygons"] = polys;
    {
        std::lock_guard<std::mutex> lk(map_overlay_mutex);
        map_overlay = j;
        has_map_overlay = true;
    }

    publish_map_overlay();
}


bool registerActions(xbot_msgs::RegisterActionsSrvRequest &req, xbot_msgs::RegisterActionsSrvResponse &res) {

    ROS_INFO_STREAM("new actions registered: " << req.node_prefix << " registered " << req.actions.size() << " actions.");

    {
        std::lock_guard<std::mutex> lk(registered_actions_mutex);
        registered_actions[req.node_prefix] = req.actions;
    }

    publish_actions();
    return true;
}

void rpc_publish_error(const int16_t code, const std::string &message, const nlohmann::basic_json<> &id = nullptr) {
    json err_resp = {{"jsonrpc", "2.0"},
                       {"error", {{"code", code}, {"message", message}}},
                       {"id", id}};
    try_publish("rpc/response", err_resp.dump(2));
}

void rpc_request_callback(const std::string &payload) {
    // Parse
    json req;
    try {
      req = json::parse(payload);
    } catch (const json::parse_error &e) {
      return rpc_publish_error(xbot_rpc::RpcError::ERROR_INVALID_JSON, "Could not parse request JSON");
    }

    // Validate
    if (!req.is_object()) {
        return rpc_publish_error(xbot_rpc::RpcError::ERROR_INVALID_REQUEST, "Request is not a JSON object");
    }
    json id = req.contains("id") ? req["id"] : nullptr;
    if (id != nullptr && !id.is_string()) {
        return rpc_publish_error(xbot_rpc::RpcError::ERROR_INVALID_REQUEST, "ID is not a string", id);
    } else if (!req.contains("jsonrpc") || !req["jsonrpc"].is_string() || req["jsonrpc"] != "2.0") {
        return rpc_publish_error(xbot_rpc::RpcError::ERROR_INVALID_REQUEST, "Invalid JSON-RPC version");
    } else if (!req.contains("method") || !req["method"].is_string()) {
        return rpc_publish_error(xbot_rpc::RpcError::ERROR_INVALID_REQUEST, "Method is not a string", req["id"]);
    }

    // Check if the method is registered
    const std::string method = req["method"];
    if (method.compare(0, 5, "meta.") == 0) {
      // Silently ignore methods that are handled by the meta service.
      return;
    }
    bool is_registered = false;
    {
        std::lock_guard<std::mutex> lk(registered_methods_mutex);
        for (const auto& [_, method_ids] : registered_methods) {
            if (std::find(method_ids.begin(), method_ids.end(), method) != method_ids.end()) {
                is_registered = true;
                break;
            }
        }
    }
    if (!is_registered) {
        return rpc_publish_error(xbot_rpc::RpcError::ERROR_METHOD_NOT_FOUND, "Method \"" + method + "\" not found", req["id"]);
    }

    // Forward to the providers as ROS message
    xbot_rpc::RpcRequest msg;
    msg.method = method;
    msg.params = req.contains("params") ? req["params"].dump() : "";
    msg.id = id != nullptr ? id : "";
    rpc_request_pub.publish(msg);
}

void rpc_response_callback(const xbot_rpc::RpcResponse::ConstPtr &msg) {
    json result;
    try {
        result = json::parse(msg->result);
    } catch (const json::parse_error &e) {
        return rpc_publish_error(xbot_rpc::RpcError::ERROR_INTERNAL, "Internal error while parsing result JSON: " + std::string(e.what()), msg->id);
    }

    if (msg->id.find("mqtt_timetable_set") == 0 || msg->id.find("mqtt_timetable_suspension_set") == 0) {
        if (result.is_object() && result.contains("valid")) {
            publish_timetable_validation(result);
        }
    }

    if (msg->id.find("mqtt_map_set") == 0) {
        if (result.is_object() && result.contains("valid")) {
            publish_map_validation(result);
        } else if (result.is_string()) {
            publish_map_validation({{"valid", true}, {"remarks", {result.get<std::string>()}}});
        } else {
            publish_map_validation({{"valid", true}, {"remarks", {"Map gespeichert"}}});
        }
    }

    json j = {{"jsonrpc", "2.0"}, {"result", result}, {"id", msg->id}};
    try_publish("rpc/response", j.dump(2));
}

void rpc_error_callback(const xbot_rpc::RpcError::ConstPtr &msg) {
    if (msg->id.find("mqtt_timetable_set") == 0 || msg->id.find("mqtt_timetable_suspension_set") == 0) {
        try {
            json validation = json::parse(msg->message);
            publish_timetable_validation(validation);
        } catch (const json::exception &) {
            publish_timetable_validation({{"valid", false}, {"remarks", {msg->message}}});
        }
    }
    if (msg->id.find("mqtt_map_set") == 0) {
        try {
            json validation = json::parse(msg->message);
            publish_map_validation(validation);
        } catch (const json::exception &) {
            publish_map_validation({{"valid", false}, {"remarks", {msg->message}}});
        }
    }
    rpc_publish_error(msg->code, msg->message, msg->id);
}

bool register_methods(xbot_rpc::RegisterMethodsSrvRequest &req, xbot_rpc::RegisterMethodsSrvResponse &res) {
    std::lock_guard<std::mutex> lk(registered_methods_mutex);
    registered_methods[req.node_id] = req.methods;
    ROS_INFO_STREAM("new methods registered: " << req.node_id << " registered " << req.methods.size() << " methods.");
    return true;
}

int main(int argc, char **argv) {
    ros::init(argc, argv, "xbot_monitoring");
    has_map = false;
    has_map_overlay = false;


    n = new ros::NodeHandle();
    ros::NodeHandle paramNh("~");

    version_string = paramNh.param("software_version", std::string("UNKNOWN VERSION"));
    if(version_string.empty()) {
        version_string = "UNKNOWN VERSION";
    }

    statustransition_log_file = paramNh.param("statustransition_log_file", std::string("/data/ros/log_statustransition.json"));
    const int configured_statustransition_log_mqtt_limit = paramNh.param("statustransition_log_mqtt_default_limit", 20);
    if (configured_statustransition_log_mqtt_limit <= 0) {
        mqtt_statustransition_log_default_limit = STATUSTRANSITION_LOG_MAX_ENTRIES;
    } else {
        mqtt_statustransition_log_default_limit = std::min<std::size_t>(
            static_cast<std::size_t>(configured_statustransition_log_mqtt_limit),
            STATUSTRANSITION_LOG_MAX_ENTRIES);
    }
    {
        std::lock_guard<std::mutex> lk(statustransition_log_mutex);
        load_statustransition_log_if_needed_locked();
    }

    external_mqtt_enable = paramNh.param("external_mqtt_enable", false);
    external_mqtt_topic_prefix = paramNh.param("external_mqtt_topic_prefix", std::string(""));
    if(!external_mqtt_topic_prefix.empty() && external_mqtt_topic_prefix.back() != '/') {
        // append the /
        external_mqtt_topic_prefix = external_mqtt_topic_prefix+"/";
    }

    mqtt_timetable_publish_interval_sec = paramNh.param("mqtt_timetable_publish_interval_sec", 60.0);
    external_mqtt_hostname = paramNh.param("external_mqtt_hostname", std::string(""));
    external_mqtt_port = std::to_string(paramNh.param("external_mqtt_port", 1883));
    external_mqtt_username = paramNh.param("external_mqtt_username", std::string(""));
    external_mqtt_password = paramNh.param("external_mqtt_password", std::string(""));

    if(external_mqtt_enable) {
        ROS_INFO_STREAM("Using external MQTT broker: " << external_mqtt_hostname << ":" << external_mqtt_port << " with topic prefix: " + external_mqtt_topic_prefix);
    }

    // First setup MQTT
    setupMqttClient();

    ros::ServiceServer register_action_service = n->advertiseService("xbot/register_actions", registerActions);

    ros::Subscriber robotStateSubscriber = n->subscribe("xbot_monitoring/robot_state", 10, robot_state_callback);
    ros::Subscriber mapSubscriber = n->subscribe("mower_map_service/json_map", 10, map_callback);
    ros::Subscriber timetableSubscriber = n->subscribe("timetable/status", 10, timetable_status_callback);
    ros::Subscriber mapOverlaySubscriber = n->subscribe("xbot_monitoring/map_overlay", 10, map_overlay_callback);

    cmd_vel_pub = n->advertise<geometry_msgs::Twist>("xbot_monitoring/remote_cmd_vel", 1);
    action_pub = n->advertise<std_msgs::String>("xbot/action", 1);

    rpc_request_pub = n->advertise<xbot_rpc::RpcRequest>(xbot_rpc::TOPIC_REQUEST, 100);
    ros::Subscriber rpc_response_sub = n->subscribe(xbot_rpc::TOPIC_RESPONSE, 100, rpc_response_callback);
    ros::Subscriber rpc_error_sub = n->subscribe(xbot_rpc::TOPIC_ERROR, 100, rpc_error_callback);
    ros::ServiceServer register_methods_service = n->advertiseService(xbot_rpc::SERVICE_REGISTER_METHODS, register_methods);

    ros::AsyncSpinner spinner(1);
    spinner.start();

    rpc_provider.init();

    ros::Rate sensor_check_rate(10.0);

    boost::regex topic_regex("/xbot_monitoring/sensors/.*/info");

    // Maps a sensor info topic to its subscriber. Only touched by this thread.
    std::map<std::string, ros::Subscriber> active_subscribers;
    std::vector<ros::Subscriber> sensor_data_subscribers;

    while (ros::ok()) {
        // Read the topics in /xbot_monitoring/sensors/.*/info and subscribe to them.
        ros::master::V_TopicInfo topics;
        ros::master::getTopics(topics);
        std::for_each(topics.begin(), topics.end(), [&](const ros::master::TopicInfo &item) {

            if (!boost::regex_match(item.name, topic_regex) || active_subscribers.count(item.name) != 0)
                return;

            ROS_INFO_STREAM("Found new sensor topic " << item.name);
            active_subscribers[item.name] = n->subscribe<xbot_msgs::SensorInfo>(
                item.name, 1, [topic = item.name, &sensor_data_subscribers](const xbot_msgs::SensorInfo::ConstPtr &msg) {
                    ROS_INFO_STREAM("Got sensor info for sensor on topic " << msg->sensor_name << " on topic " << topic);

                    bool is_new = false;
                    {
                        std::unique_lock<std::mutex> lk(found_sensors_mutex);
                        is_new = found_sensors.count(topic) == 0;

                        // Sensor already known and sensor-info equals?
                        if (!is_new && found_sensors[topic] == *msg) return;

                        found_sensors[topic] = *msg;  // Save the (new|changed) sensor info
                    }

                    // Let the info subscription alive for dynamic threshold changes
                    //active_subscribers.erase(topic);  // Stop subscribing to infos

                    if (is_new) {
                        subscribe_to_sensor(topic, sensor_data_subscribers);  // Subscribe for data
                    }

                    // Republish (new|changed) sensor info
                    // NOTE: If a sensor name or id changes, the related data topic wouldn't change!
                    //       But do we dynamically change a sensor name or id?
                    publish_sensor_metadata();
                }
            );
        });
        maybe_publish_timetable(false);
        sensor_check_rate.sleep();
    }
    return 0;
}
