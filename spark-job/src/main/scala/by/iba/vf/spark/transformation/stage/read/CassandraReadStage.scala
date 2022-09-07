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
import by.iba.vf.spark.transformation.stage.{CassandraStageConfig, ReadStageBuilder, Stage, StageBuilder}
import org.apache.spark.sql.cassandra.DataFrameReaderWrapper
import org.apache.spark.sql.{DataFrame, SparkSession}

private[read] final class CassandraReadStage(override val id: String,
                                             cassandraStageConfig: CassandraStageConfig,
                                            ) extends ReadStage(id, CassandraStageConfig.storageId) {
  override def read(implicit spark: SparkSession): DataFrame = {
    cassandraStageConfig.applyConfig(spark)
    spark.read.cassandraFormat(cassandraStageConfig.table, cassandraStageConfig.keyspace, cassandraStageConfig.cluster.getOrElse(""), cassandraStageConfig.pushdownEnabled).load
  }

  override val builder: StageBuilder = CassandraReadStageBuilder
}

object CassandraReadStageBuilder extends ReadStageBuilder {
  override def expectedStorage: String = CassandraStageConfig.storageId

  override protected def validateRead(config: Map[String, String]): Boolean =
    CassandraStageConfig.validateConfig(config)

  override protected def convert(config: Node): Stage = {
    val cassandraConfig = new CassandraStageConfig(config)
    new CassandraReadStage(config.id, cassandraConfig)
  }
}
