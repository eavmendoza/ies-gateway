if [[ "$1" == "tilt" ]]; then
	SENSOR="AXL"
elif [[ "$1" == "soil" ]]; then
	SENSOR="SMS"
elif [[ "$1" == "batt" ]]; then
	SENSOR="GTW1\$BTV"
else
	echo "ERROR: Sensor type" $1
	exit 1
fi

if [[ -z $2 ]]; then
	echo "ERROR: No sensor ID given"
	exit 1
fi

if [[ -z $3 ]]; then
	n=20
else
	n=$3
fi

sudo mysql -uroot edcrpidb -e "select dt,message from transactions where message like '%$SENSOR$2%' ORDER BY dt DESC LIMIT $n"
