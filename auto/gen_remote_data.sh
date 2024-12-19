#!/usr/bin/bash
. gedi_ssh.cfg

echo "Generating sql files on remote servers"

# use dos2unix first if newly uploaded file

DELAY=14
if [[ -z "$1" ]]; then
	echo "Switching to default 14 days DELAY"
	DELAY=14
else
	DELAY=$1
	echo "Generating previous $DELAY data"
fi

function gen_magbu () {
	expect $WORK_DIR/_gen_sql_magbu.exp $DELAY
}

function gen_bgbu () {
	expect $WORK_DIR/_gen_sql_bgbu.exp $DELAY
}

function gen_lgbu () {
	expect $WORK_DIR/_gen_sql_lgbu.exp $DELAY
}

function gen_nigbu () {
	expect $WORK_DIR/_gen_sql_nigbu.exp $DELAY
}

if [[ -z "$2" ]]; then
	echo "Generating raw data for ALL sites"
	expect $WORK_DIR/_gen_sql_bgbu.exp $DELAY &
	expect $WORK_DIR/_gen_sql_lgbu.exp $DELAY &
	expect $WORK_DIR/_gen_sql_nigbu.exp $DELAY &
	expect $WORK_DIR/_gen_sql_magbu.exp $DELAY &
	wait
elif [[ "$2" == "magbu" ]]; then
	echo "Generating raw data for magbu"
	expect /home/ies/gateway/auto/_gen_sql_magbu.exp $DELAY
elif [[ "$2" == "nigbu" ]]; then
	echo "Generating raw data for nigbu"
	expect /home/ies/gateway/auto/_gen_sql_nigbu.exp $DELAY
elif [[ "$2" == "lgbu" ]]; then
	echo "Generating raw data for lgbu"
	expect /home/ies/gateway/auto/_gen_sql_lgbu.exp $DELAY
elif [[ "$2" == "bgbu" ]]; then
	echo "Generating raw data for bgbu"
	expect /home/ies/gateway/auto/_gen_sql_bgbu.exp $DELAY
else
	echo "ERRORL: Unknown site $2"
fi

