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
import by.iba.vf.spark.transformation.stage.RedisStageConfig._
import by.iba.vf.spark.transformation.stage.{ReadStageBuilder, RedisStageConfig, Stage, StageBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}

class RedisReadStage(override val configNode: Node,
                     redisConfig: RedisStageConfig,
                    ) extends ReadStage(configNode, RedisStageConfig.storageId) {
  override def read(implicit spark: SparkSession): DataFrame = {
    var options: Map[String, String] = redisConfig.provideConnectionOptions
    List(
      (tableActualName, redisConfig.table),
      (keyColumnActualName, redisConfig.keyColumn),
      (modelActualName, redisConfig.model),
      (keysPatternActualName, redisConfig.keysPattern),
    ).foreach {
      case (optName, Some(opt)) => options += (optName -> opt)
      case (_, _) =>
    }
    if (readModePattern.equals(redisConfig.readMode.getOrElse(throw new TransformationConfigurationException("Read mode wasn't specified")))) {
      options += ("infer.schema" -> "true")
    }
    spark.read.format("org.apache.spark.sql.redis").options(options).load
  }

  override val builder: StageBuilder = RedisReadStageBuilder
}

object RedisReadStageBuilder extends ReadStageBuilder {
  override def expectedStorage: String = RedisStageConfig.storageId

  override protected def validateRead(config: Map[String, String]): Boolean =
    RedisStageConfig.validateConfig(config) && config.contains(readModeFieldName) &&
      (
        (readModeKey.equals(config(readModeFieldName)) && config.contains(tableFieldName)) ||
          (readModePattern.equals(config(readModeFieldName)) && config.contains(keysPatternFieldName))
        )

  override protected def convert(config: Node): Stage = {
    new RedisReadStage(config, new RedisStageConfig(config))
  }
}
