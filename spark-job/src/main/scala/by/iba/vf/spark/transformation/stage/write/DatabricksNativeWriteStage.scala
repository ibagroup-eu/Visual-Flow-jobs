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
import by.iba.vf.spark.transformation.stage.{DatabricksNativeStageConfig, Stage, StageBuilder, WriteStageBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DatabricksNativeWriteStage(override val configNode: Node, config: DatabricksNativeStageConfig, options: Map[String, String])
  extends WriteStage(configNode, DatabricksNativeStageConfig.storageId){

  override def write(df: DataFrame)(implicit spark: SparkSession): Unit =
    config.objectType.foreach {
      case "table" => writeTable(df)
      case "volume" => writeVolume(df)
    }

  private def writeTable(df: DataFrame): Unit =
    df.write
      .mode(config.saveMode.getOrElse("error"))
      .options(config.options ++ options)
      .partitionBy(config.partitionBy: _*)
      .saveAsTable(config.getFullyQualifiedTable)

  private def writeVolume(df: DataFrame): Unit =
    df.write
      .format(config.format.getOrElse("delta"))
      .mode(config.saveMode.getOrElse("error"))
      .options(config.options ++ options)
      .partitionBy(config.partitionBy: _*)
      .save(config.getFullyQualifiedObject)

  override val builder: StageBuilder = DatabricksNativeWriteStageBuilder
}

object DatabricksNativeWriteStageBuilder extends WriteStageBuilder {
  override def expectedStorage: String = DatabricksNativeStageConfig.storageId

  override protected def validateWrite(config: Map[String, String]): Boolean = {
    val objectType = config.get(DatabricksNativeStageConfig.fieldObjectType)
    config.contains(DatabricksNativeStageConfig.fieldCatalog) &&
      config.contains(DatabricksNativeStageConfig.fieldSchema) &&
      config.contains(DatabricksNativeStageConfig.fieldObjectType) &&
      objectType.exists {
        case "table" => validateTableScenario(config)
        case "volume" => validateVolumeScenario(config)
        case _ => throw new TransformationConfigurationException(s"Invalid value '$objectType' of the parameter 'object type'. Allowed values: 'table', 'volume'.")
      } &&
      config.contains(DatabricksNativeStageConfig.fieldWriteMode)
  }

  def validateTableScenario(config: Map[String, String]): Boolean =
    config.contains(DatabricksNativeStageConfig.fieldTable)

  def validateVolumeScenario(config: Map[String, String]): Boolean =
    config.contains(DatabricksNativeStageConfig.fieldVolume) &&
      config.contains(DatabricksNativeStageConfig.fieldFormat) &&
      config.contains(DatabricksNativeStageConfig.fieldPath)

  override protected def convert(config: Node): Stage =
    new DatabricksNativeWriteStage(config, new DatabricksNativeStageConfig(config), getOptions(config.value))
}