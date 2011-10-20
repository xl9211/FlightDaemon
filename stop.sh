#!/bin/bash

pid=`ps -ef | fgrep "FlightDaemon" |fgrep -v "fgrep" | awk '{print $2}'`
echo $pid

if [ $pid ]
then
	kill -9 $pid 
fi
