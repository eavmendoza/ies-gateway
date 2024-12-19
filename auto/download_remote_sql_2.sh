#!/usr/bin/bash

# read config file
. gedi_ssh.cfg

if [[ -z "$1" ]]; then
	today=$(date -d 'today' "+%Y%m%d")
else
	today=$1
fi

function dl_magbu () {
	SITE_NAME="magbu"
	echo -e "\n###############\nDownloading ${SITE_NAME} files"
	SERVER_NAME="ubuntu@local@mtapo-slope:SSH"
	SERVER_DIR="/home/ubuntu/downloads"
	SENSORS_FNAME="${SITE_NAME}_logs_sensors_"
	NETWORK_FNAME="${SITE_NAME}_logs_network_"
	# sshpass -p $PASS scp -oUser=$SERVER_NAME:$USER $HOST:$SERVER_DIR/\{$SENSORS_FNAME$today.sql,$NETWORK_FNAME$today.sql\} $LOCAL_DOWNLOADS_DIR
	# sshpass -p$PASS scp -oUser=$SERVER_NAME:$USER $HOST:$SERVER_DIR/$SENSORS_FNAME$today.sql $LOCAL_DOWNLOADS_DIR
	sshpass -p$PASS scp -oUser=$SERVER_NAME:$USER $HOST:$SERVER_DIR/$NETWORK_FNAME$today.sql $LOCAL_DOWNLOADS_DIR
	sshpass -p $PASS scp -oUser=$SERVER_NAME:$USER $HOST:$SERVER_DIR/$SENSORS_FNAME$today.sql $LOCAL_DOWNLOADS_DIR

}

function dl_bgbu () {
	SITE_NAME="bgbu"
	echo -e "\n###############\nDownloading ${SITE_NAME} files"
	SERVER_NAME="amhmendoza.e@local@bacman_slope:SSH"
	SERVER_DIR="/home/amhmendoza.e/downloads"
	SENSORS_FNAME="${SITE_NAME}_logs_sensors_"
	NETWORK_FNAME="${SITE_NAME}_logs_network_"
	sshpass -p$PASS scp -oUser=$SERVER_NAME:$USER $HOST:$SERVER_DIR/$SENSORS_FNAME$today.sql $LOCAL_DOWNLOADS_DIR
	sshpass -p$PASS scp -oUser=$SERVER_NAME:$USER $HOST:$SERVER_DIR/$NETWORK_FNAME$today.sql $LOCAL_DOWNLOADS_DIR
	# sshpass -p $PASS scp -oUser=$SERVER_NAME:$USER $HOST:$SERVER_DIR/\{$SENSORS_FNAME$today.sql,$NETWORK_FNAME$today.sql\} $LOCAL_DOWNLOADS_DIR

}

function dl_nigbu () {
	SITE_NAME="nigbu"
	echo -e "\n###############\nDownloading ${SITE_NAME} files"
	expect nigbu_download.exp $today

}


function dl_lgbu () {
	SITE_NAME="lgbu"
	echo -e "\n###############\nDownloading ${SITE_NAME} files"
	expect lgbu_download.exp $today
}

if [[ -z "$2" ]]; then
	dl_magbu &
	dl_nigbu &
	dl_lgbu &
	dl_nigbu &
	wait
elif [[ "$2" == "magbu" ]]; then
	dl_magbu
elif [[ "$2" == "nigbu" ]]; then
	dl_nigbu
elif [[ "$2" == "lgbu" ]]; then
	dl_lgbu
elif [[ "$2" == "bgbu" ]]; then
	dl_bgbu
else
	echo "ERROR: Unknow site $2"
fi

