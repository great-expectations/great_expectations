#!/bin/bash

# The xenial image is required for python 3.7, but openjdk8 cannot be manually
# specified in that distro. However, pyspark requires JDK8, so, manually install it
# and set java_home.
# See: 
# https://travis-ci.community/t/how-to-use-java8-in-a-python-non-java-project-on-xenial/1823

# show current JAVA_HOME and java version
echo "Current JAVA_HOME: $JAVA_HOME"
echo "Current java -version:"
java -version

# install Java 8
sudo add-apt-repository -y ppa:openjdk-r/ppa
sudo apt-get -qq update
sudo apt-get install -y openjdk-8-jdk --no-install-recommends
sudo update-java-alternatives -l
sudo update-java-alternatives -s java-1.8.0-openjdk-amd64
ls -l /usr/lib/jvm/
ls -l /usr/lib/jvm/java-8-openjdk-amd64
# source /opt/jdk_switcher/jdk_switcher
/opt/jdk_switcher/jdk_switcher use openjdk8

# change JAVA_HOME to Java 8
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
echo "Current JAVA_HOME: $JAVA_HOME"
echo "Current java -version:"
source ~/.bash_profile.rc
java -version