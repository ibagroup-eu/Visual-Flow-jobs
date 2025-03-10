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
import by.iba.vf.spark.transformation.exception.TransformationConfigurationException
import by.iba.vf.spark.transformation.stage.RedshiftStageConfig.{customSql, database, query}
import by.iba.vf.spark.transformation.stage.{ReadStageBuilder, RedshiftStageConfig, Stage, StageBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}

class RedshiftReadStage(override val configNode: Node, config: RedshiftStageConfig)
  extends ReadStage(configNode, RedshiftStageConfig.storageId) {
  override def read(implicit spark: SparkSession): DataFrame = {
    config.setUpConfigParams(spark.sparkContext)
    var options = config.provideConnectionOptions
    if (config.redshiftCustomSql) {
      options += ("query" -> config.redshiftQuery.getOrElse(
        throw new TransformationConfigurationException("'query' parameter is missing, even though custom sql mode is turned on")
      ))
    } else {
      options += (RedshiftStageConfig.dbtable -> config.redshiftTable.getOrElse(
        throw new TransformationConfigurationException("'table' parameter is missing")
      ))
    }
    spark.read.format("io.github.spark_redshift_community.spark.redshift").options(options).load
  }

  override val builder: StageBuilder = RedshiftReadStageBuilder
}

object RedshiftReadStageBuilder extends ReadStageBuilder {
  override def expectedStorage: String = RedshiftStageConfig.storageId

  override protected def validateRead(config: Map[String, String]): Boolean = {
    val customSqlMode = config.get(customSql).exists(x => x.toBoolean)
    RedshiftStageConfig.validateConfig(config) &&
      ((customSqlMode && config.contains(query)) || (!customSqlMode && config.contains(RedshiftStageConfig.table)))
  }


  override protected def convert(config: Node): Stage =
    new RedshiftReadStage(config, new RedshiftStageConfig(config))
}
