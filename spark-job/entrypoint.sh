#!/bin/bash

#
# Copyright (c) 2021 IBA Group, a.s. All rights reserved.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -ex
JOB_JAR="/opt/spark/work-dir/spark-transformations-0.1-jar-with-dependencies.jar"

exec /opt/spark/bin/spark-submit \
  --master "$JOB_MASTER" \
  --driver-memory "$DRIVER_MEMORY" \
  --driver-cores "$DRIVER_CORES" \
  --executor-memory "$EXECUTOR_MEMORY" \
  --executor-cores "$EXECUTOR_CORES" \
  --deploy-mode "client" \
  --name "$POD_NAME" \
  --driver-class-path "$JOB_JAR" \
  --conf spark.sql.shuffle.partitions="$SHUFFLE_PARTITIONS" \
  --conf spark.executor.extraClassPath="$JOB_JAR" \
  --conf spark.kubernetes.namespace="$POD_NAMESPACE" \
  --conf spark.kubernetes.container.image="$JOB_IMAGE" \
  --conf spark.kubernetes.driver.request.cores="$DRIVER_REQUEST_CORES" \
  --conf spark.kubernetes.driver.limit.cores="$DRIVER_CORES" \
  --conf spark.kubernetes.executor.request.cores="$EXECUTOR_REQUEST_CORES" \
  --conf spark.kubernetes.executor.limit.cores="$EXECUTOR_CORES" \
  --conf spark.kubernetes.container.image.pullSecrets="$IMAGE_PULL_SECRETS" \
  --conf spark.kubernetes.container.image.pullPolicy="Always" \
  --conf spark.executor.instances="$EXECUTOR_INSTANCES" \
  --conf spark.driver.host="$POD_IP" \
  --conf spark.driver.port=14536 \
  --conf spark.kubernetes.executor.label.jobId="$JOB_ID" \
  --conf spark.kubernetes.executor.label.pipelineJobId="$PIPELINE_JOB_ID" \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim."$PVC_NAME".mount.path="$MOUNT_PATH" \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim."$PVC_NAME".mount.readOnly=false \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim."$PVC_NAME".options.claimName="$PVC_NAME" \
  "$JOB_JAR" \
  -Dlog4j2.configurationFile=/opt/spark/conf/log4j.properties
