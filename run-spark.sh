#!/bin/sh
set -ex
wget https://www.apache.org/dyn/closer.lua/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz
tar -xzvf spark-2.4.3-bin-hadoop2.7.tgz
cd spark-2.4.3-bin-hadoop2.7 && ./sbin/start-master.sh &