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
import by.iba.vf.spark.transformation.stage.{WriteStageBuilder, SnowflakeStageConfig, Stage, StageBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}

class SnowflakeWriteStage(override val configNode: Node, config: SnowflakeStageConfig)
  extends WriteStage(configNode, SnowflakeStageConfig.storageId) {

  override def write(df: DataFrame)(implicit spark: SparkSession): Unit =
    df.write.format("net.snowflake.spark.snowflake").options(config.parameter).mode(SaveMode.Overwrite).save()

  override val builder: StageBuilder = SnowflakeWriteStageBuilder
}

object SnowflakeWriteStageBuilder extends WriteStageBuilder {
  override def expectedStorage: String = SnowflakeStageConfig.storageId

  override protected def validateWrite(config: Map[String, String]): Boolean = {
    config.contains(SnowflakeStageConfig.sfURLFieldName) && config.contains(SnowflakeStageConfig.sfUserFieldName) &&
    (config.contains(SnowflakeStageConfig.sfPasswordFieldName) || config.contains(
      SnowflakeStageConfig.pemPrivateKeyFieldName
    ) ||
      (config.contains(SnowflakeStageConfig.sfAuthenticatorFieldName) && config.contains(
        SnowflakeStageConfig.sfTokenFieldName
      ))) &&
    config.contains(SnowflakeStageConfig.sfDatabaseFieldName) && config.contains(SnowflakeStageConfig.sfSchemaFieldName)
  }

  override protected def convert(config: Node): Stage =
    new SnowflakeWriteStage(config, new SnowflakeStageConfig(config))
}
