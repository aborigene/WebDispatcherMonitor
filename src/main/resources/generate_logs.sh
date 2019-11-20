#!/bin/bash

rm -rf target/WebDispatcher/WebDispatcher*log
LOG=`cat /Users/igor.simoes/Documents/Customers/Gerdau\ -\ IBM/WebDispatcher/Logs/wu2_log0/access_log-2019-09-18.log`
I=0
while read -r line; do 
	echo $line >> WebDispatcher/WebDispatcher09-11-2019.log 
	echo $line
	sleep 1
	I=$(($I + 1))
	if [ $I -eq 4 ]; then
		echo "Rotating..."
		mv WebDispatcher/WebDispatcher09-11-2019.log WebDispatcher/WebDispatcher09-11-2019-1.log
		I=0;
	fi
done < /Users/igor.simoes/Documents/Customers/Gerdau\ -\ IBM/WebDispatcher/Logs/wu2_log0/access_log-2019-09-18.log
