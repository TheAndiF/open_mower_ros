OpenMower ROS - MQTT Status Transition Log
Stand: 2026-05-15

Enthaltene Erweiterung:
- Statuswechsel-Log in xbot_monitoring
- Persistente JSON-Datei: /data/ros/log_statustransition.json
- MQTT Publish Topic: statustransition_log/json
- MQTT Renew Topic: statustransition_log/set/renew/json
- Default MQTT-Ausgabe: 20 Eintraege
- Optionales Renew-Payload: {"limit": 50}
- Maximal persistierte Eintraege: 300

Zentrale geaenderte Datei:
- src/lib/xbot_monitoring/src/xbot_monitoring.cpp

Einspielen in laufenden Container, wenn nur die Datei aktualisiert werden soll:
1. docker cp src/lib/xbot_monitoring/src/xbot_monitoring.cpp open_mower_ros:/opt/open_mower_ros/src/lib/xbot_monitoring/src/xbot_monitoring.cpp
2. docker exec -it open_mower_ros bash -ic 'cd /opt/open_mower_ros && catkin build xbot_monitoring'
3. docker restart open_mower_ros

Pruefung danach:
- docker exec -it open_mower_ros bash -ic 'strings /opt/open_mower_ros/devel/lib/xbot_monitoring/xbot_monitoring | grep statustransition'
- docker exec -it open_mower_ros bash -ic 'ls -l /data/ros/log_statustransition.json'
