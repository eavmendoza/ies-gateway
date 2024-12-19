#!/bin/bash
# automated backup
echo mysql backup routine

if [[ -z "$1" ]]; then
        start_dt=$(date -d 'today -2 days' "+%Y-%m-%d")
else
        start_dt=$(date -d "today -$1 days" "+%Y-%m-%d")
fi

today=$(date -d 'today' "+%Y%m%d")
echo 'Start dt' $start_dt

#echo $1
#echo $2

BU='bgbu'
#start_dt=$(date -d 'today - 14 days' "+%Y-%m-%d")
today=$(date -d 'today' "+%Y%m%d")
#echo 'Start dt' $start_dt

sudo mysqldump -uroot edcslopedb gateway_network_status --where="stat_datetime>'${start_dt}' " --no-create-info | sed -e "s/([0-9]*,/(NULL,/gi; s/INSERT/INSERT IGNORE/g;" > /home/amhmendoza.e/downloads/${BU}_logs_network_$today.sql
sudo mysqldump -uroot edcslopedb position_sensor_logs power_sensor_logs rain_gauge_sensor_logs soil_moisture_sensor_logs --where="log_datetime>'${start_dt}' " --no-create-info  | sed -e "s/([0-9]*,/(NULL,/gi; s/INSERT/INSERT IGNORE/g;" > /home/amhmendoza.e/downloads/${BU}_logs_sensors_$today.sql

echo 'Done'