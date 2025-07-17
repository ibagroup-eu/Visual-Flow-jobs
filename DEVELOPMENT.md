# Set up development environment

## Requirements

1) Make sure that you have the following software installed

| Software                   | Version                                                             | Purpose                                                                 |
|----------------------------|---------------------------------------------------------------------|-------------------------------------------------------------------------|
| Java                       | 11                                                                  | To supplement Scala which is used in spark jobs                         |
| Scala                      | 2.12.12                                                             | To work with spark jobs                                                 |
| Maven                      | 3+                                                                  | To manage spark jobs' dependencies                                      |
| Python                     | 3.8                                                                 | To work with slack jobs                                                 |
| pip                        | should be built into Python, if not then install the latest version | To manage slack jobs' dependencies                                      |
| Spark                      | 3.4.1                                                               | To run spark jobs                                                       |
| Kubernetes CLI(optional)   | actual for target cluster                                           | To be able to run spark and slack jobs in a production-like environment |

2) If you want to run slack jobs(on any environment), make sure that you have these environment variables set

| Name                    | Explanation                                   |
|-------------------------|-----------------------------------------------|
| LOGGER_CONFIG_FILE_PATH | Absolute path to a log file for the slack job |
| SLACK_API_TOKEN         | Token to be able to work with Slack API       |

3) If you want to run spark jobs, make sure that you have these environment variables set

| Name                   | Explanation                                                                                                     |
|------------------------|-----------------------------------------------------------------------------------------------------------------|
| JOB_CONFIG             | Job configuration which consists of the stages. Check [stage_fields.md](./stage_fields.md) for more information |

# How to run

## Slack jobs
Regardless of the environment you want to run this in, it's pretty simple.
- Make sure that you have python installed
- Ensure that you have necessary env variables in place
- Run the slack job via standard python command providing necessary CLI arguments

_If you want to run the slack job in kubernetes environment, check out [Dockerfile](./slack-job/Dockerfile)_

## Spark jobs
There are 3 most common ways of running spark jobs:

## 1. In kubernetes via [backend API](https://github.com/ibagroup-eu/Visual-Flow-backend)
This requires you to have a properly configured and running backend API that has the connection to the Kubernetes server you want to run this on.

In order to run the job you have to:
- create/pick a Visual Flow job in backend API
- run it through a specific endpoint

The API takes care of Pod creation/configuration and will execute _spark-submit_ for you via [entrypoint.sh](./spark-job/entrypoint.sh). 

Entrypoint script uses quite a lot of environment variables. You can find their description below.

| Name                                                  | Explanation                                                                                  |
|-------------------------------------------------------|----------------------------------------------------------------------------------------------|
| JOB_MASTER                                            | URL to kubernetes cluster API                                                                |
| DRIVER_MEMORY                                         | Amount of memory to use for the driver process                                               |
| DRIVER_CORES                                          | Number of cores to use for the driver process, only in cluster mode                          |
| DRIVER_REQUEST_CORES                                  | Specify the cpu request for the driver pod                                                   |
| EXECUTOR_MEMORY                                       | Amount of memory to use per executor process                                                 |
| EXECUTOR_CORES                                        | The number of cores to use on each executor                                                  |
| EXECUTOR_REQUEST_CORES                                | Specify the cpu request for each executor pod                                                |
| EXECUTOR_INSTANCES                                    | Number of executors                                                                          |
| IMAGE_PULL_SECRETS                                    | Comma separated list of Kubernetes secrets used to pull images from private image registries |
| SHUFFLE_PARTITIONS                                    | The default number of partitions to use when shuffling data for joins or aggregations        |
| POD_IP                                                | Hostname or IP address for the driver                                                        |
| POD_NAME                                              | The name of your application                                                                 |
| POD_NAMESPACE                                         | The namespace that will be used for running the driver and executor pods                     |
| JOB_IMAGE                                             | Container image to use for the Spark application                                             |
| JOB_ID                                                | Executor label for job's id                                                                  |
| PIPELINE_JOB_ID                                       | Executor label for stage's id                                                                |
| JOB_JAR                                               | Path to spark job jar                                                                        |
| VISUAL_FLOW_CONFIGURATION_TYPE                        | Runtime configuration type: Kubernetes (default) or Databricks                               |
| VISUAL_FLOW_RUNTIME_MODE                              | Runtime mode type: SILENT (default, regular Spark job) or INTERACTIVE (for debugging)        |
| BACKEND_HOST                                          | Backend host to interact with Backend for Frontend service (BFF)                             |
| JOB_CONFIG                                            | Job config JSON value                                                                        |
| JOB_CONFIG_PATH                                       | Path to job config, if it's passed via file on the disk                                      |
| VISUAL_FLOW_DATABRICKS_SECRET_SCOPE                   | Secret scope name in Databricks workspace for storing Visual Flow's secrets                  |
| VISUAL_FLOW_DATABRICKS_ARTIFACTS_PATH                 | Path in Databricks Unity Catalog for storing Visual Flow's artifacts                         |
| JOB_DEFINITION_POLL_INTERVAL_IN_MILLISECONDS          | Time interval in milliseconds how often to poll updates from BFF service (1000 by default)   |
| INTERACTIVE_MODE_TOTAL_PROCESSING_ITERATIONS_ALLOWED  | Maximum number of poll cycles allowed in the Interactive mode (24 * 3600 by default)         |

## 2. In kubernetes by executing _spark-submit_ locally
This requires you to have:
- spark installed locally
- Kubernetes CLI with proper .kube/config file
- compiled spark jobs as a .jar file

You'll have to manually create the pod and set up all necessary environment variables(especially the one that holds job configuration).
Then you may use [entrypoint.sh](./spark-job/entrypoint.sh) as the example of _spark-submit_ command that you have to execute.
## 3. Executing _spark-submit_ locally
This requires you to have:
- spark installed locally
- compiled spark jobs as a .jar file

Make sure to set up all necessary environment variables(especially the one that holds job configuration).

Then just execute _spark-submit_ command with "client" _deploy-mode_ and "local" _master_. You can find the example of command for local execution below:

```bash
spark-submit \
  --master "local" \
  --deploy-mode "client" \
  "$JOB_JAR"
```
_$JOB_JAR is the path to spark-jobs .jar_

# How to develop a new connector

1) Add connection's driver as a Maven dependency in the [pom.xml](./spark-job/pom.xml).
2) Add the corresponding read and write modules in the [read](./spark-job/src/main/scala/by/iba/vf/spark/transformation/stage/read) and [write](./spark-job/src/main/scala/by/iba/vf/spark/transformation/stage/write) packages to specify logic for read and write stage respectively.
3) In the read stage add the following:
  * Read stage builder object extending ReadStageBuilder with implementation of the following members:
    * expectedStorage - storage ID
    * validateRead - logic to validate whether this stage matches parameter set (it's called when parsing job configuration and matching config nodes to stages)
    * convert - builds stage from job configuration node
  * Write stage builder object extending ReadStageBuilder with implementation of the following members:
    * expectedStorage - storage ID
    * validateRead - logic to validate whether this stage matches parameter set (it's called when parsing job configuration and matching config nodes to stages)
    * convert - builds stage from job configuration node
  * Read stage class extending ReadStage with implementation of the following members:
    * read - logic to read data from the data source and returning Spark data frame
  * Write stage class extending ReadStage with implementation of the following members:
    * write - logic to write Spark data frame to the target data store
4) Add config class and object companions in the [stage](./spark-job/src/main/scala/by/iba/vf/spark/transformation/stage) package to define some connection parameters and their parsing logic.
5) Add read and write stage builders into the list of StageBuilders in the [ExecutionPlanBuilder](./spark-job/src/main/scala/by/iba/vf/spark/transformation/plan/ExecutionPlanBuilder.scala) module.
6) Test locally.
7) Apply the corresponding changes in frontend to make newly added connection available in UI.

**_NOTE:_**  Use existing stages as examples for reference.
