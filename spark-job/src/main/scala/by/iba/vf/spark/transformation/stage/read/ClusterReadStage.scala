/*
 * Copyright (c) 2021 IBA Group, a.s. All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package by.iba.vf.spark.transformation.stage.read

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.stage.ClusterStageConfig.{formatFieldName, pathFieldName}
import by.iba.vf.spark.transformation.stage.{ClusterStageConfig, ReadStageBuilder, Stage, StageBuilder}
import by.iba.vf.spark.transformation.stage.read.ObjectStorageReadS3StageBuilder.getOptions
import org.apache.spark.sql.{DataFrame, SparkSession}

class ClusterReadStage(override val configNode: Node, var options: Map[String, String], config: ClusterStageConfig)
  extends ReadStage(configNode, ClusterStageConfig.storageId) {

  override def read(implicit spark: SparkSession): DataFrame = {
    if (config.format == "avro" && !config.useSchema)
      options = options.filter(elem => elem._1 != "avroSchema")
    spark.read.options(options).format(config.format).load(s"/files/${config.path}")
  }

  override val builder: StageBuilder = ClusterReadStageBuilder
}

object ClusterReadStageBuilder extends ReadStageBuilder {
  override def expectedStorage: String = ClusterStageConfig.storageId

  override protected def validateRead(config: Map[String, String]): Boolean =
    config.contains(pathFieldName) && config.contains(formatFieldName)

  override protected def convert(config: Node): Stage = {
    new ClusterReadStage(config, getOptions(config.value), new ClusterStageConfig(config))
  }
}
