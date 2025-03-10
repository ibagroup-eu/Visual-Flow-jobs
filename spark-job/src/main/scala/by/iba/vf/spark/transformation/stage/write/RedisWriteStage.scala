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
import by.iba.vf.spark.transformation.stage.RedisStageConfig._
import by.iba.vf.spark.transformation.stage.{RedisStageConfig, Stage, StageBuilder, WriteStageBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}

class RedisWriteStage(override val configNode: Node,
                      redisConfig: RedisStageConfig,
                     ) extends WriteStage(configNode, RedisStageConfig.storageId) {
  override def write(df: DataFrame)(implicit spark: SparkSession): Unit = {
    var options: Map[String, String] = redisConfig.provideConnectionOptions
    options += (tableActualName -> redisConfig.table.getOrElse(throw new TransformationConfigurationException("Table name was not provided")))
    List(
      (keyColumnActualName, redisConfig.keyColumn),
      (modelActualName, redisConfig.model),
      (ttlActualName, redisConfig.ttl)
    ).foreach {
      case (optName, Some(opt)) => options += (optName -> opt)
      case (_, _) =>
    }
    val dfWriter = getDfWriter(df, redisConfig.saveMode)
    dfWriter.format("org.apache.spark.sql.redis").options(options).save
  }

  override val builder: StageBuilder = RedisWriteStageBuilder
}


object RedisWriteStageBuilder extends WriteStageBuilder {
  override def expectedStorage: String = RedisStageConfig.storageId

  override protected def validateWrite(config: Map[String, String]): Boolean = {
    RedisStageConfig.validateConfig(config) && config.contains(tableFieldName) && config.contains(writeModeFieldName)
  }

  override protected def convert(config: Node): Stage = {
    new RedisWriteStage(config, new RedisStageConfig(config))
  }
}
