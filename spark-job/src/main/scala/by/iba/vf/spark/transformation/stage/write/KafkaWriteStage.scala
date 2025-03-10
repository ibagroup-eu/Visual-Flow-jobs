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
import by.iba.vf.spark.transformation.stage.{KafkaStageConfig, Stage, StageBuilder, WriteStageBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}

class KafkaWriteStage(override val configNode: Node, config: KafkaStageConfig, options: Map[String, String])
  extends WriteStage(configNode, KafkaStageConfig.storageId){

  override def write(df: DataFrame)(implicit spark: SparkSession): Unit = {
    df.write
      .format("kafka")
      .option("kafka.bootstrap.servers", config.bootstrapServers)
      .option("topic", config.topic.orNull)
      .options(options)
      .save()
  }

  override val builder: StageBuilder = KafkaWriteStageBuilder
}

object KafkaWriteStageBuilder extends WriteStageBuilder {
  override def expectedStorage: String = KafkaStageConfig.storageId

  override protected def validateWrite(config: Map[String, String]): Boolean = {
    config.contains(KafkaStageConfig.bootstrapServersFieldName) &&
      config.contains(KafkaStageConfig.topicFieldName)
  }

  override protected def convert(config: Node): Stage =
    new KafkaWriteStage(config, new KafkaStageConfig(config), getOptions(config.value))
}