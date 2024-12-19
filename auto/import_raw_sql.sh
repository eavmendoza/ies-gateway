#!/usr/bin/bash

. gedi_ssh.cfg

if [[ -z "$1" ]]; then
  today=$(date -d 'today' "+%Y%m%d")
else
  today=$1
fi

echo $today
files=$(ls $LOCAL_DOWNLOADS_DIR | grep $today)

i=1;
while read n; do
  echo "$n"

  # mysql -uiesuser -h192.168.0.189 edcslopedb < $LOCAL_DOWNLOADS_DIR$n --password="$(cat .mpass)"
  mysql -uiesuser -h192.168.0.164 edcslopedb < $LOCAL_DOWNLOADS_DIR$n --password="$(cat .mpass)" &
  # if [[ "$n" == "Pqr def" ]]; then
  #     echo "--- $n was found at line $i"
  # fi
  i=$(($i+1));
done <<< "$files"

wait