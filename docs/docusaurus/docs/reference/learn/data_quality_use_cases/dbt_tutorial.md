---
sidebar_label: 'dbt Tutorial'
title: 'Using GX with dbt'
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Overview

This tutorial will enable you to schedule and run data pipelines locally using PostgreSQL as the database, dbt for transforming data, Great Expectations for data quality and Airflow for workflow orchestration - all running inside of containers via Docker Compose. This tutorial assumes you have basic knowledge of Python, SQL, Docker, and how to use a CLI. We will have step-by-step instructions along with explanations for those who want a deeper understanding of what we are doing and why.

Before we get started, let’s take a look at a high-level overview of how these tools will work together. First, notice that we are using Docker Compose, which allows us to run each of the tools/services below in separate Docker containers that can depend on and interact with each other. Containerizing our services is a best practice as it allows us to customize the environment for each service and replicate it on any other machine.

As mentioned above, we have PostgreSQL to store our data, dbt to transform it, Great Expectations to run data quality tests, and Airflow to orchestrate that whole process. All of these tools are popular open-source tools that you are likely to find in the real world, but we encourage you to focus on the concepts rather than the specifics of these tools.

Finally, to interact with our data, we will be using pgAdmin to query/view it (though you can use another database query tool if you want), the Great Expectations data docs to see our data quality results, Airflow to run our pipeline, and a CLI to interact with some of the services. 


## Requirements

In addition to the requirements below, we recommend using an IDE like VSCode to organize your project.

[Docker Desktop](https://www.docker.com/get-started/)
[ssh-keygen](https://www.ssh.com/academy/ssh/keygen)
Bash shell or Z shell (Zsh) 

## Clone the github repo

Open a terminal window and navigate to a project folder where you’d like to create this tutorial. We have created a template that you will clone via github repo. Clone the repo and create the rest of the folder structure by running the steps below.

```bash
git clone git@github.com:greatexpectationslabs/dbt-tutorial.git
cd dbt-tutorial/tutorial-dbt-gx-airflow
mkdir data
mkdir great-expectations
mkdir ssh-keys
```

After completing the steps above, your folder structure should look like this:

dbt-tutorial
└── tutorial-dbt-gx-airflow/
    ├── airflow/
    │   └── .env
    ├── data/
    ├── great-expectations/
    ├── pgadmin-data/
    ├── ssh-keys/
    ├── airflow.Dockerfile

Let's take a moment to explain some of the contents of the configuration we have just cloned.

### Airflow configuration file (airflow/.env)
- The first 3 variables under `# Meta-Database` set the PostgreSQL credentials for Airflow's metadata storage. Note that this database is strictly for Airflow to store its metadata and it’s not the same PostgreSQL database that we will be using for storing our actual data. You should change any default usernames and passwords like this for a production environment.
- `AIRFLOW__CORE__EXECUTOR` : Because we are running this locally, we are using the `LocalExecutor`, but in production you would typically use a remote executor like the Celery Executor or the Kubernetes Executor.
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`: This is a connection string to the Airflow metadata database mentioned above.
Similar to the first bullet, the last 2 variables set the credentials you’ll need to login to the airflow webserver.

### Airflow Dockerfile
- Here we use the official airflow image as a base image and install the GX airflow provider so Airflow can interact with GX.

### dbt Project Template
- We included the [jaffle shop](https://github.com/dbt-labs/jaffle-shop) template project from GitHub that has all of the configuration files we need to get started, but we needed to tweak some of the settings from the example project.
- We added a `.env` file to store the credentials needed to connect to PostgreSQL.
- Next, we told our dbt project to use a default profile containing the credentials needed to connect to our PostgreSQL database. We use dbt Jinja syntax here to pass in sensitive credentials from our .env file.

### dbt Dockerfile
- Inside the `dbt.Dockerfile`, we tell Docker to copy our dbt folder into the container and install dbt dependencies. In this case, we install dbt-postgres since we are using PostgreSQL as our database.
- We added a line (line 11) to export the credentials from our credentials file when we connect to the Docker container. This is not the only way to achieve this, but just one example.
- We install ssh which will allow us to run an ssh server inside this container so that we can connect to this container via ssh from other containers. We expose the port 22 because that is the default port that SSH expects to use. Finally, we run a command to tell ssh that our ssh key is authorized so we don’t have to deal with any verification prompts when interacting programmatically.

### Great Expectations Dockerfile
- We instruct Docker to install a few libraries we will need for GX and for it to interact with PostgreSQL.

### Docker Compose File
There’s a lot to unpack here, so we will cover the main points:
- Starting on line 97, you’ll notice that Airflow is a collection of 4 different services: an initialization service (run the first time we run the service), a webserver (the website we connect to to see our pipelines), a scheduler, and a PostgreSQL backend (stores metadata about pipelines).
- For each service, we are defining the Docker image to use (some we made, some are on DockerHub), environment variables to pass into the container, and which services are dependent on each other. For example, we need our airflow-postgres service to be running before we can start the webserver and scheduler services.
- On line 159, you will see a network section where we define a default network for all of the services to communicate on. This also allows us to use the service names as the host name in credentials instead of an ip address.
- Finally, on line 165, we specify Docker secrets to show how to securely pass in our ssh keys into the containers that will use them.

## Generate SSH Keys
You will need to generate a key pair that you will use in our Docker services to securely authenticate with each other. You can read more about how SSH keys work [here](https://www.foxpass.com/blog/learn-ssh-keys-in-minutes/).

Run the following command in a terminal window inside of the `tutorial-dbt-gx-airflow` directory:
```bash
ssh-keygen -t rsa -b 4096 -f ./ssh-keys/id_rsa -N ""
```
Double check that at this point, your project structure looks like this:

dbt-tutorial/
└── tutorial-dbt-gx-airflow/
    ├── airflow/
    │   └── .env
    ├── data/
    ├── dbt/
    │   └── …
    ├── great expectations/
    ├── pgadmin-data/
    ├── ssh-keys/
    │   └── …
    ├── airflow.Dockerfile
    ├── dbt.Dockerfile
    ├── docker-compose.yaml
    └── gx.Dockerfile

## Start the Docker Services
We are now ready to start up our services and start working with them. In the next few steps we will also show you how to connect to each service and work with it. Make sure you have Docker Desktop open and running before running the docker commands.

Using a command prompt in your `tutorial-dbt-gx-airflow` directory, run the following to start all of the services:

```bash
docker compose up -d --build
```

When you run this for the first time, it may take several minutes to download and install the required libraries. You will also notice that one of the services `airflow-init` exits after running, but that is expected because it’s only used to initialize the airflow service. Once everything is complete, your Docker Desktop Containers screen will look like this:

## Connect to the Database
One of the services we are running with Docker is pgAdmin, which will let you interact with the PostgreSQL database. We will be using pgAdmin in this tutorial, but feel free to use a SQL editor of your choice instead. 

### Log into pgAdmin
You can open pgAdmin by either clicking on the port for the pgAdmin service in Docker Desktop or by going to [http://localhost:15433](http://localhost:15433) in your browser. Log in using the credentials specified in docker compose for pgAdmin:

- Username: example@email.com
- Password: postgres

### Create a connection to PostgreSQL
- Right click on Servers in the top left corner and choose Register > Server
- Name it postgres
- Click on the Connection tab
    - Host name/address: database (refers to the database docker service)
    - Username and password: postgres (defined in Docker compose under database service)
- Save the connection

You will now be able to see your empty database when you look under the public schema > tables:

## Populate the Database 
The dbt project that we included from GitHub has some initial datasets in the form of [seed files](https://docs.getdbt.com/docs/build/seeds). We will import these seed files into our database as tables and then use these tables as the data source for the rest of our dbt models.

Open the dbt Docker container’s terminal via running the following command in your project’s terminal:
```bash
docker exec -it dbt bash -l
```
> Here we are using the docker exec command to open an interactive terminal inside the container using bash as the shell. The -l flag is important here because it tells the shell to be a login shell which automatically sources the .bashrc file we put into the container which exports the dbt environment variables. See the dbt.Dockerfile for more info.

Once inside the dbt container, run the `dbt deps` command to install dependencies. Next, copy the seed data into the seeds folder by running `cp jaffle-data/*.csv` and then run the `dbt seed` command to import the data into the PostgreSQL database. By default, it will import the data into a new schema called `jaffle_shop_raw`. This step might take a few minutes to complete.

```bash
dpt deps
cp jaffle-data/*.csv
dbt seed
```

In pgAdmin you can now see the newly created tables in the new schema.

Run the rest of the dbt models by running the command below in the dbt service command line:
```bash
dbt run
```

Again, we can see the models successfully created as tables and views but in our public schema:

## Use Great Expectations to create Data Quality Tests
Now that we have some data to test in our database, let’s set up GX so we can create some data quality tests and run them against the data.

We will be following the [GX getting started guide](https://docs.greatexpectations.io/docs/oss/get_started/get_started_with_gx_and_sql), but running it inside Docker. There are a number of ways you can create the GX configuration. In this tutorial, we will use a Jupyter Notebook, but you can also use your preferred Python development tool instead.

### Initialize the Jupyter Notebook Server
Run the following command in our project’s terminal to start the Jupyter Notebook Server:
```bash
docker exec great-expectations jupyter notebook --allow-root --no-browser --ip=0.0.0.0
```

Once it’s up and running you will see a similar message to the one below and be able to copy or click on the url starting with 127.0.0.1. The token at the end of the URL is unique and required to access the Jupyter Notebook server:

Create a new notebook. In the top left corner choose File > New > Notebook. Then select the Python 3 Kernel if prompted.

### Create the GX Configuration
Copy the following code into the notebook. We recommend breaking up each part into different cells to see the outputs when you run each section. The overview of what we are doing here is creating a Data Context, which stores your GX configuration, creating a Data Source (connecting to PostgreSQL), creating a Data Asset (defining one of the tables to test on - the `customers` table in PostgreSQL), creating Expectations for the data (`ExpectColumnValuesToNotBeNull` and `ExpectColumnValuesToBeBetween`), and finally running Validations.

Start by naming the Notebook `customer_expectations.ipynb`, or a name of your choosing. Copy each of the following code blocks into separate cells in your notebook and run them one at a time using CTRL + Enter or SHIFT + Enter:

```python
import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.checkpoint.actions import UpdateDataDocsAction

context = gx.get_context(mode="file")
```
We first import a few modules that we’ll need to use in our GX configuration, as well as create the Data Context. In our example, we will store our configuration to the file system.

> You can either press the b button on your keyboard or press the “Insert a cell below” button in any cell to create a new cell.

```python
## Connect to your data

PG_CONNECTION_STRING = "postgresql+psycopg2://postgres:postgres@database/postgres"
pg_datasource = context.data_sources.add_postgres(name="pg_datasource", connection_string=PG_CONNECTION_STRING)
asset = pg_datasource.add_table_asset(name="customer_data", table_name="customers")
bd = asset.add_batch_definition_whole_table("BD")
```
A Data Source is the GX representation of a data store. In this tutorial, we are using a PostgreSQL data source, but you can also connect to others, including Pandas, Snowflake and Databricks. A Data Asset is the GX representation of a collection of records within a Data Source which are usually grouped based on the underlying data system. In this tutorial, we create a Batch Definition for the whole `customers` table, but you can also create batches based on a date column within the table.

```python
## Create Expectations
suite = context.suites.add(gx.ExpectationSuite("Suite"))
vd = gx.ValidationDefinition(
    name="Validation Definition",
    data=bd,
    suite=suite
)

context.validation_definitions.add(vd)
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="customer_id"))
suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="lifetime_spend", min_value=0, max_value=100000))
```
An Expectation Suite is a group of Expectations that describe how data should be tested. A Validation Definition simply links an Expectation Suite to the data it describes via the Batch Definition we created in the previous step. We can then add Expectations to the Expectation Suite. In this example, we create an Expectation to check that the `customer_id` column values are never null. Separately, we create an Expectation to check that the values of the `lifetime_spend` column are between 0 and 100000. Take a look at the [Expectations Gallery](https://greatexpectations.io/expectations/) to see the other Expectations you can create!

```python
## Validate your data
checkpoint = context.checkpoints.add(gx.Checkpoint(
    name="Checkpoint",
    validation_definitions=[vd],
    actions=[
        UpdateDataDocsAction(name="update_data_docs")
    ]
))

checkpoint_result = checkpoint.run()
```
A Checkpoint is an object that groups Validation Definitions and runs them with shared parameters and automated Actions. Checkpoints are the primary means for validating data in a production deployment of Great Expectations. In this tutorial, we update our Data Docs, which is a static website generated from Great Expectations metadata detailing Expectations, Validation Results, etc.

### View the Expectation results in Data Docs
You’ve now run these data quality tests manually by executing the Notebook, which will allow you to view the test results via the local data docs site.

Open the data docs site here: [http://127.0.0.1:8888/edit/gx/uncommitted/data_docs/local_site/index.html](http://127.0.0.1:8888/edit/gx/uncommitted/data_docs/local_site/index.html)

Now you can view your Validation results.

You can see that the two Expectations that we have created are passing. Whenever Expectations fail, the status column will be updated to indicate a failure, along with the Observed Value column to include any values that have fallen outside of the expected range.

## Build a data pipeline and automate it with Airflow
At this point we have verified that each service is working correctly so far. The last step is to now automate this process with a pipeline or dag in our workflow orchestration tool - Airflow. We will make a simple pipeline using the common write-audit-publish pattern.

### Log into Airflow
Open [http://localhost:8080](http://localhost:8080) in your browser. You can use the username and password we created earlier in our configuration step.
username=airflow
password=airflow

And you will see an empty dags dashboard:

### Create a DAG and add a connection
Create a new Airflow dag using the command in the root of your project directory:

```bash
touch airflow/dags/customers_dag.py
```

Copy the contents below into your new dag file:

```python
from datetime import datetime
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.operators.python import PythonOperator
import great_expectations as gx

sshHook = SSHHook(ssh_conn_id="dbt-ssh", cmd_timeout=None)

def run_gx_checkpoint():
   context = gx.get_context(mode="file", project_root_dir="../../great-expectations/")
   context.checkpoints.get("Checkpoint").run()
  
with DAG(
   dag_id='customers_dag',
   start_date=datetime(2023, 11, 10),
   schedule_interval=None
) as dag:

   write_data = SSHOperator(
       task_id='write_data',
       command="dbt seed",
       ssh_hook=sshHook,
   )

   gx_run_audit = PythonOperator(
       task_id="gx_run_audit",
       python_callable=run_gx_checkpoint
   )

   dbt_publish_model = SSHOperator(
       task_id='dbt_publish_model',
       command="dbt run",
       ssh_hook=sshHook,
   )

write_data >> gx_run_audit >> dbt_publish_model
```
You can see that in this DAG file, we use the same commands that we executed manually earlier. The main point to note here is that executing a GX Checkpoint is as simple as loading the Data Context that we previously created and then running the Checkpoint that has been stored within it.

It might take a few minutes for the new pipeline to show up in Airflow, but you will also see an error saying our dbt connection isn’t defined, so let’s create it now. We will also need to add the credentials for our PostgreSQL database for the Great Expectations step. If you do not see the error, refresh the Airflow dashboard page in your browser.

Add dbt-ssh credentials:
- In Airflow click on Admin > Connections
- Click on the + to add a new record
    - Fill out the connection using the following:
    - Connection id: dbt-ssh
    - Connection Type: SSH
    - Host: dbt
    - Username: root

Add Postgres credentials:
- Still inside Admin > Connections click + to add a new record
- Fill out the connection using the following:
    - Connection id: postgres
    - Connection Type: Postgres
    - Host: database
    - Schema: postgres
    - Login: postgres
    - Password: postgres
    - Port: 5432

Your connections page will now look like this:

You will now see `customers_dag` listed on the DAGs page:

### Run the DAG

Run the DAG by clicking on the play button under Actions and then choosing Trigger DAG.

> Note: If you see an error saying “Task exited with return code Negsignal.SIGKILL” then it usually means that Airflow doesn’t have enough resources to run. Airflow recommends 4GB memory. Make sure your Docker resources are set appropriately (Docker Desktop > settings > Resources.)

You can click on the dag name to watch it as it runs and wait for it to complete:

Refresing the Data Docs page will show the new results from the DAG run:

## Conclusion
Congratulations on completing this tutorial using PostgreSQL, dbt, GX and Airflow! By now, you should have an understanding of how to schedule and run data pipelines using some great open source tools. We strongly encourage you to explore what else you can do within GX. For example, you can try connecting to your own Data Sources, or create other Expectations by referring to our [Expectations Gallery](https://greatexpectations.io/expectations/). 

If you are ready to take a step beyond what is covered here and collaborate with your team, be sure to check out [GX Cloud](https://greatexpectations.io/cloud), where you can use our online platform run GX.
