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
package by.iba.vf.spark.transformation.stage.write

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.exception.TransformationConfigurationException
import by.iba.vf.spark.transformation.stage.{RedshiftStageConfig, Stage, StageBuilder, WriteStageBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}

class RedshiftWriteStage(override val configNode: Node, config: RedshiftStageConfig)
  extends WriteStage(configNode, RedshiftStageConfig.storageId) {
  override def write(df: DataFrame)(implicit spark: SparkSession): Unit = {
    config.setUpConfigParams(spark.sparkContext)
    var options = config.provideConnectionOptions
    options += (RedshiftStageConfig.dbtable -> config.redshiftTable.getOrElse(
      throw new TransformationConfigurationException("'table' parameter is missing")
    ))
    val dfWriter = getDfWriter(df, config.saveMode)
    dfWriter.format("io.github.spark_redshift_community.spark.redshift").options(options).save
  }

  override val builder: StageBuilder = RedshiftWriteStageBuilder
}

object RedshiftWriteStageBuilder extends WriteStageBuilder {
  override def expectedStorage: String = RedshiftStageConfig.storageId

  override protected def validateWrite(config: Map[String, String]): Boolean = {
    RedshiftStageConfig.validateConfig(config) &&
      config.contains(RedshiftStageConfig.table) && config.contains(RedshiftStageConfig.writeModeFieldName)
  }

  override protected def convert(config: Node): Stage = {
    new RedshiftWriteStage(config, new RedshiftStageConfig(config))
  }
}
