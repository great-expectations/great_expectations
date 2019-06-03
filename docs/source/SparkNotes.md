## Working Spark Notes

### Deploy mode
Spark can execute in three kinds of deploy modes:
- standalone: spark master and workers are started on nodes; master provides a URL for workers to connect to \
and to which a spark context can be connected, using something like \
`SparkConf conf = new SparkConf().setMaster("spark://<master url>:7077").setAppName("Word Count")`. \
Note that this mode is *not* supported by Amazon EMR. [See here](https://aws.amazon.com/premiumsupport/knowledge-center/emr-submit-spark-job-remote-cluster/)
- client (default YARN mode): relies on YARN scheduler on the cluster. "In client mode the Spark driver (and SparkContext) runs on a client node outside a YARN cluster" --> that means that the node where the client runs is the one on which the relevant libraries need to be installed.
- cluster: in cluster mode the Spark driver (and SparkContext) runs inside a YARN cluster, i.e. inside a YARN container alongside ApplicationMaster (that acts as the Spark application in YARN).

[See here](https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-deploy-mode.html)

### Launching a cluster
EMR allows launching Spark easily. Notes:

 - Overall AWS Docs are good re: launching spark as part of the cluster.
I was not able to find good documentation re: installing libraries into the pyspark kernels enabled when Jupyterhub is installed.
I resorted to running pyspark on the master node and installing additional libraries (i.e. great expectations) there.

 - Accessing the cluster in general requires using ssh tunnels. The documentation on the cluster admin page is strong for that.
 I was able to launch a cluster in our private subnet and connect via our VPN easily (yay!).


### Running notebooks

There are two modes for running notebooks:
1. Console-enabled jupyter instance. However "Installing additional library packages from within the notebook editor is not currently supported. If you require customized kernels or library packages, install them using bootstrap actions or by specifying a custom Amazon Linux AMI for Amazon EMR when you create a cluster. For more information, see Create Bootstrap Actions to Install Additional Software and Using a Custom AMI."
(as of 20190522; https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-managed-notebooks-considerations.html). In that case, we likely want to advise using bootstrap actions for AWS EMR-based spark clusters to add libraries.

2. Jupyterhub. Jupyterhub must be installed separately, and to add libraries to its kernels requires accessing the docker container in which it runs on the master node. \
Even then, to use the spark contexts (which use Apache Livy), we'd still need to bootstrap installation of dependencies on the cluster.
