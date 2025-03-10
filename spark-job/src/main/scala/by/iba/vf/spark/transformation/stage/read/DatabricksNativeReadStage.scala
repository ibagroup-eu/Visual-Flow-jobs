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
import by.iba.vf.spark.transformation.stage.{DatabricksNativeStageConfig, ReadStageBuilder, Stage, StageBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DatabricksNativeReadStage(override val configNode: Node, config: DatabricksNativeStageConfig, options: Map[String, String])
  extends ReadStage(configNode, DatabricksNativeStageConfig.storageId){

  override def read(implicit spark: SparkSession): DataFrame =
    config.objectType.map {
      case "table" => readTable(spark)
      case "volume" => readVolume(spark)
    }.getOrElse(spark.emptyDataFrame)

  private def readTable(spark: SparkSession): DataFrame =
    spark.read
      .options(config.options ++ options)
      .table(config.getFullyQualifiedTable)

  private def readVolume(spark: SparkSession): DataFrame =
    spark
      .read
      .format(config.format.getOrElse("delta"))
      .options(config.options ++ options)
      .load(config.getFullyQualifiedObject)

  override val builder: StageBuilder = DatabricksNativeReadStageBuilder
}

object DatabricksNativeReadStageBuilder extends ReadStageBuilder {
  override def expectedStorage: String = DatabricksNativeStageConfig.storageId

  override protected def validateRead(config: Map[String, String]): Boolean = {
    val objectType = config.get(DatabricksNativeStageConfig.fieldObjectType)
    config.contains(DatabricksNativeStageConfig.fieldCatalog) &&
      config.contains(DatabricksNativeStageConfig.fieldSchema) &&
      config.contains(DatabricksNativeStageConfig.fieldObjectType) &&
      objectType.exists {
        case "table" => validateTableScenario(config)
        case "volume" => validateVolumeScenario(config)
        case _ => throw new TransformationConfigurationException(s"Invalid value '$objectType' of the parameter 'object type'. Allowed values: 'table', 'volume'.")
      }
  }

  def validateTableScenario(config: Map[String, String]): Boolean =
    config.contains(DatabricksNativeStageConfig.fieldTable)

  def validateVolumeScenario(config: Map[String, String]): Boolean =
    config.contains(DatabricksNativeStageConfig.fieldVolume) &&
    config.contains(DatabricksNativeStageConfig.fieldFormat) &&
    config.contains(DatabricksNativeStageConfig.fieldPath)


  override protected def convert(config: Node): Stage =
    new DatabricksNativeReadStage(config, new DatabricksNativeStageConfig(config), getOptions(config.value))
}