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
import by.iba.vf.spark.transformation.stage.{ClickhouseStageConfig, Stage, StageBuilder, WriteStageBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.sql.DriverManager

class ClickhouseWriteStage(override val configNode: Node, config: ClickhouseStageConfig)
  extends WriteStage(configNode, ClickhouseStageConfig.storageId) {

  override def write(df: DataFrame)(implicit spark: SparkSession): Unit = {
    if (config.write_mode.orNull.equals("Overwrite")) {
      truncate(config.table.orNull, config.parameter("url"), config.user, config.password, config.parameter("driver"))
      getDfWriter(df, Some("append")).format("jdbc").options(config.parameter).save()
    } else if (config.write_mode.orNull.equals("Append")) {
      getDfWriter(df, Some("append")).format("jdbc").options(config.parameter).save()
    }
  }

  def truncate(tableName: String, jdbcUrl: String, username: String, password: String, driver: String): Boolean = {
    Class.forName(driver)
    val connection = DriverManager.getConnection(jdbcUrl, username, password)
    connection.setAutoCommit(true)
    val statement = connection.createStatement()
    statement.execute(s"TRUNCATE TABLE $tableName")
  }

  override val builder: StageBuilder = ClickhouseWriteStageBuilder
}

object ClickhouseWriteStageBuilder extends WriteStageBuilder {
  override def expectedStorage: String = ClickhouseStageConfig.storageId

  override protected def validateWrite(config: Map[String, String]): Boolean = {
    config.contains(ClickhouseStageConfig.hostFieldName) && config.contains(ClickhouseStageConfig.portFieldName) &&
      config.contains(ClickhouseStageConfig.passwordFieldName) && config.contains(ClickhouseStageConfig.userFieldName) &&
      config.contains(ClickhouseStageConfig.databaseFieldName) && config.contains(ClickhouseStageConfig.schemaFieldName) &&
      config.contains(ClickhouseStageConfig.tableFieldName) && config.contains(ClickhouseStageConfig.writeModeFieldName)
  }

  override protected def convert(config: Node): Stage = {
    new ClickhouseWriteStage(config, new ClickhouseStageConfig(config))
  }
}