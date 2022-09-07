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
import by.iba.vf.spark.transformation.stage.CassandraStageConfig.writeModeFieldName
import by.iba.vf.spark.transformation.stage.{CassandraStageConfig, Stage, StageBuilder, WriteStageBuilder}
import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class CassandraWriteStage(override val id: String,
                          cassandraStageConfig: CassandraStageConfig,
                         ) extends WriteStage(id, CassandraStageConfig.storageId) {
  override def write(df: DataFrame)(implicit spark: SparkSession): Unit = {
    cassandraStageConfig.applyConfig(spark)
    cassandraStageConfig.writeMode.foreach(sm => {
      if (SaveMode.Overwrite.toString.equals(sm)) {
        spark.conf.set("confirm.truncate", "true")
      }
    })
    val dfWriter = getDfWriter(df, cassandraStageConfig.writeMode)
    dfWriter.cassandraFormat(cassandraStageConfig.table, cassandraStageConfig.keyspace, cassandraStageConfig.cluster.getOrElse(""), cassandraStageConfig.pushdownEnabled).save
  }

  override val builder: StageBuilder = CassandraWriteStageBuilder
}

object CassandraWriteStageBuilder extends WriteStageBuilder {
  override def expectedStorage: String = CassandraStageConfig.storageId

  override protected def validateWrite(config: Map[String, String]): Boolean = {
    CassandraStageConfig.validateConfig(config) && config.contains(writeModeFieldName)
  }

  override protected def convert(config: Node): Stage = {
    val cassandraConfig = new CassandraStageConfig(config)
    new CassandraWriteStage(config.id, cassandraConfig)
  }
}
