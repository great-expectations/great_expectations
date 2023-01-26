Spark possesses a few dependencies that need to be installed before it can be used with AWS.  You will need to install the `aws-java-sdk-bundle` and `hadoop-aws` files corresponding to your version of pySpark, and update your Spark configuration accordingly.  You can find the `.jar` files you need to install in the following MVN repositories:

- [hadoop-aws jar that matches your Spark version](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws)
- [aws-java-sdk-bundle jar that matches your Spark version](aws-java-sdk-bundle jar that matches your Spark version)

Once the dependencies are  installed, you will need to update your Spark configuration from within Python.  First, import these necessary modules:

```python
import pyspark as pyspark
from pyspark import SparkContext
```

Next, update the `pyspark.SparkConf` to match the dependency packages you downloaded:

```python
conf = pyspark.SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.1')
```

Finally, you will need to add your AWS credentials to the `SparkContext`.

```python
sc = SparkContext(conf=conf)
sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', [AWS ACCESS KEY])
sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', [AWS SECRET KEY])
```