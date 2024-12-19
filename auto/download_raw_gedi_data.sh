#!/bin/bash
# automated backup
echo Creating gedi raw data backup file


start_dt=$(date -d 'today - 14 days' "+%Y-%m-%d")
today=$(date -d 'today' "+%Y%m%d")
echo 'Start dt' $start_dt

sudo mysqldump -uroot edcrpidb transactions --where="dt>'${start_dt}' " --no-create-info | sed -e "s/([0-9]*,/(NULL,/gi; s/INSERT/INSERT IGNORE/g;" > ~/downloads/gedi_raw_data_$today.sql

echo 'Done'