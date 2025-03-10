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
import by.iba.vf.spark.transformation.stage.{KafkaStageConfig, ReadStageBuilder, Stage, StageBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}

class KafkaReadStage(override val configNode: Node, config: KafkaStageConfig, options: Map[String, String])
  extends ReadStage(configNode, KafkaStageConfig.storageId){

  override def read(implicit spark: SparkSession): DataFrame = {
    spark.read.format("kafka")
      .option("kafka.bootstrap.servers", config.bootstrapServers)
      .option("subscribe", config.subscribe.orNull)
      .option("startingOffsets", "earliest")
      .options(options)
      .load()
  }

  override val builder: StageBuilder = KafkaReadStageBuilder
}

object KafkaReadStageBuilder extends ReadStageBuilder {
  override def expectedStorage: String = KafkaStageConfig.storageId

  override protected def validateRead(config: Map[String, String]): Boolean = {
    config.contains(KafkaStageConfig.bootstrapServersFieldName) &&
      config.contains(KafkaStageConfig.subscribeFieldName)
  }

  override protected def convert(config: Node): Stage =
    new KafkaReadStage(config, new KafkaStageConfig(config), getOptions(config.value))
}