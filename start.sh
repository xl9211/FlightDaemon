#!/bin/bash
date=`date +"%Y-%m-%d_%H:%M:%S"`
cp log/fd.log log/fd.log.$date
python FlightDaemon.py conf/fd.cfg 1>&2 2>log/fd.log &
