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
import by.iba.vf.spark.transformation.stage.JdbcDatabricksStageBuilder
import by.iba.vf.spark.transformation.stage.ReadStageBuilder
import by.iba.vf.spark.transformation.stage.Stage
import by.iba.vf.spark.transformation.stage.StageBuilder
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

private[read] final class JdbcDatabricksReadStage(
  configNode: Node,
  fullyQualifiedTable: String,
  truststorePath: Option[String],
  jdbcConfig: Map[String, String],
  options: Map[String, String],
  customSql: Boolean
) extends JdbcReadStage(
  configNode: Node,
  fullyQualifiedTable: String,
  truststorePath: Option[String],
  jdbcConfig: Map[String, String],
  options: Map[String, String],
  customSql: Boolean
) {

  override val builder: StageBuilder = JdbcDatabricksReadStageBuilder

}

object JdbcDatabricksReadStageBuilder extends JdbcDatabricksStageBuilder with ReadStageBuilder {
  final protected val fieldCustomSQL = "customSql"

  override def validateStorage(config: Map[String, String]): Boolean =
    config.get(fieldStorageId).exists(s => drivers.contains(s.toLowerCase))

  override protected def validateRead(config: Map[String, String]): Boolean =
    validateJdbc(config) && config.contains(fieldCustomSQL) &&
      ((!config(fieldCustomSQL).toBoolean && config.contains(fieldCatalog) && config.contains(fieldSchema) && config.contains(fieldTable)) ||
          (config(fieldCustomSQL).toBoolean && config.contains("option.".concat(JDBCOptions.JDBC_TABLE_NAME))))

  override protected def convert(config: Node): Stage = {
    val (fullyQualifiedTable, map) = jdbcParams(config)
    val truststorePathOption = if (config.value.contains(fieldCertData)) Some(truststorePath) else None
    new JdbcDatabricksReadStage(config, fullyQualifiedTable, truststorePathOption, map, getOptions(config.value), config.value(fieldCustomSQL).toBoolean)
  }
}
