#!/bin/bash

HOST="wab-vitro-01.energy.com.ph"

# read config file
. gedi_ssh.cfg

echo $PASS


if [[ "$1" == "bgbu" ]]; then
	USER="amhmendoza.e@local@bacman_slope:SSH:mendoza.e-amh"
elif [[ "$1" == "nigbu" ]]; then
	USER="Interactive@Negros-Slope:SSH:mendoza.e-amh"
elif [[ "$1" == "magbu" ]]; then
	USER="ubuntu@local@mtapo-slope:SSH:mendoza.e-amh"
elif [[ "$1" == "lgbu" ]]; then
	USER="ubuntu@local@Leyte_slope:SSH:mendoza.e-amh"
else
	echo "ERROR: Site name not found: " $1
	exit 1
fi

sshpass -v -p $PASS ssh -l $USER wab-vitro-01.energy.com.ph