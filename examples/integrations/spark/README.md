This Spark integration is provided as an example. Using it will also introduce dependency on the requirements listed in
the "requirements_spark.txt" file in the "great_expectations/examples/integrations/spark" directory.

Please make sure that the file "great_expectations/examples/data/Titanic.csv" exists.  This is the raw source data set.

To run the example:

* change directories into "great_expectations/examples/integrations/spark" and then execute
* pip install -r requirements_spark.txt
* pytest tests/test_titanic_csv_pieline.py

on the command line.  The result should be that both validations (before and after processing the data sets) pass.

You also must have Apache Spark installed locally.


## Installation of Apache Spark

Step 1) Prerequisites

Download and install the appropriate Java 8 SDK for your operating system:
<https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html>
Run `java -version` to validate Java installation.

The post <https://stackoverflow.com/questions/21964709/how-to-set-or-change-the-default-java-jdk-version-on-os-x> can be useful for ensuring that the correct name of the java environment is used.  According to this article, if your system has multiple version of Java installed, adding the following to your `~/.bash_profile` file may be helpful:
```sh
#Java8
export JAVA_HOME=`/usr/libexec/java_home -v 1.8.0_212`
```
Note that it may be necessary to delete a Java installation from your machine if its name conflicts with that of the Java 8 SDK that is recommended above.

Download and unzip Scala 2.12.8 into your `~/Library/Local/SoftwareDevelopment/` directory (create the intermediate
directories as necessary): <https://www.scala-lang.org/download/>
(Click on "Download the Scala binaries for macos")

Download and unzip Spark 2.4.2 into your `~/Library/Local/SoftwareDevelopment/` directory (create the intermediate
directories as necessary): <https://spark.apache.org/downloads.html>
(If 2.4.2 is not available then get the latest and replace "2.4.2" in the instructions below)

Add Scala and Spark to your system PATH by adding this to your `~/.bash_profile`:
```sh
#Scala
export SCALA_HOME=~/Library/Local/SoftwareDevelopment/scala-2.11.12
export PATH=$PATH:$SCALA_HOME/bin

#Apache Spark
export SPARK_HOME=~/Library/Local/SoftwareDevelopment/spark-2.4.3-bin-hadoop2.7
export PATH=$PATH:$SPARK_HOME/bin
```

`source ~/.bash_profile` to refresh $PATH.

