#!/usr/bin/bash

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

if [[ -z "$2" ]]; then
	echo "Generating raw data for ALL sites"
	expect /home/ies/gateway/auto/_gen_sql_bgbu.exp $DELAY
	expect /home/ies/gateway/auto/_gen_sql_lgbu.exp $DELAY
	expect /home/ies/gateway/auto/_gen_sql_nigbu.exp $DELAY
	expect /home/ies/gateway/auto/_gen_sql_magbu.exp $DELAY
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

# expect /home/ies/gateway/auto/_gen_sql_bgbu.exp $DELAY
# expect /home/ies/gateway/auto/_gen_sql_lgbu.exp $DELAY
# expect /home/ies/gateway/auto/_gen_sql_nigbu.exp $DELAY
# expect /home/ies/gateway/auto/_gen_sql_magbu.exp $DELAY

# bash /home/ies/gateway/auto/download.sh
# bash /home/ies/gateway/auto/import_raw_sql.sh
# python3 /home/ies/gateway/analysis/gedi_plots.py