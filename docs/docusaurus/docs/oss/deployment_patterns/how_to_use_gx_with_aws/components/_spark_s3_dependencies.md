Spark possesses a few dependencies that need to be installed before it can be used with AWS.  You will need to install the `aws-java-sdk-bundle` and `hadoop-aws` files corresponding to your version of pySpark, and update your Spark configuration accordingly.  You can find the `.jar` files you need to install in the following MVN repositories:

- [hadoop-aws jar that matches your Spark version](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws)
- [aws-java-sdk-bundle jar that is compatible with your Spark version](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-bundle)

Once the JAR files are downloaded, they can be added to the Spark configuration in a number of ways: 
  1. Being [added directly](https://sparkbyexamples.com/spark/add-multiple-jars-to-spark-submit-classpath/) to the `SparkConf`. 
  2. Being [added to the properties](https://sparkbyexamples.com/spark/add-multiple-jars-to-spark-submit-classpath/) specified in the [`spark-defaults.conf` file](https://spark.apache.org/docs/latest/configuration.html#dynamically-loading-spark-properties)
  3. Being copied into the `/jars/` folder of your `pyspark` installation (ie. `lib/python3.9/site-packages/pyspark/jars/`).
