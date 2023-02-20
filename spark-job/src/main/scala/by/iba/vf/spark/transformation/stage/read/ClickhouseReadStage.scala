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
import by.iba.vf.spark.transformation.stage.{ClickhouseStageConfig, ReadStageBuilder, Stage, StageBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ClickhouseReadStage(override val id: String, config: ClickhouseStageConfig)
  extends ReadStage(id, ClickhouseStageConfig.storageId){

  override def read(implicit spark: SparkSession): DataFrame =
    spark.read.format("jdbc").options(config.parameter).load()


  override val builder: StageBuilder = ClickhouseReadStageBuilder
}

object ClickhouseReadStageBuilder extends ReadStageBuilder {
  override def expectedStorage: String = ClickhouseStageConfig.storageId

  override protected def validateRead(config: Map[String, String]): Boolean = {
    config.contains(ClickhouseStageConfig.hostFieldName) && config.contains(ClickhouseStageConfig.portFieldName) &&
      config.contains(ClickhouseStageConfig.passwordFieldName) && config.contains(ClickhouseStageConfig.userFieldName) &&
      config.contains(ClickhouseStageConfig.databaseFieldName) && config.contains(ClickhouseStageConfig.customSqlFieldName) &&
      (config.contains(ClickhouseStageConfig.queryFieldName) ||
        (config.contains(ClickhouseStageConfig.tableFieldName) &&  config.contains(ClickhouseStageConfig.schemaFieldName)))
  }

  override protected def convert(config: Node): Stage =
    new ClickhouseReadStage(config.id, new ClickhouseStageConfig(config))
}