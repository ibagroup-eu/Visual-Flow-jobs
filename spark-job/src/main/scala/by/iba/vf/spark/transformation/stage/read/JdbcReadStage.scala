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
import by.iba.vf.spark.transformation.stage.JdbcStageBuilder
import by.iba.vf.spark.transformation.stage.ReadStageBuilder
import by.iba.vf.spark.transformation.stage.Stage
import by.iba.vf.spark.transformation.stage.StageBuilder
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

private[read] final class JdbcReadStage(
    override val id: String,
    schemaTable: String,
    truststorePath: Option[String],
    jdbcConfig: Map[String, String],
    options: Map[String, String],
    customSql: Boolean
) extends ReadStage(id, JdbcReadStageBuilder.jdbcStorage) {

  override val builder: StageBuilder = JdbcReadStageBuilder

  override def read(implicit spark: SparkSession): DataFrame = {
    implicit val sc: SparkContext = spark.sparkContext
    val config = if (customSql && options.contains(JDBCOptions.JDBC_TABLE_NAME))
      jdbcConfig + (JDBCOptions.JDBC_QUERY_STRING -> options(JDBCOptions.JDBC_TABLE_NAME)) else
      jdbcConfig + (JDBCOptions.JDBC_TABLE_NAME -> schemaTable)
    truststorePath.foreach(sc.addFile)

    spark.read.format(JdbcReadStageBuilder.jdbcStorage).options(config).load()
  }
}

object JdbcReadStageBuilder extends JdbcStageBuilder with ReadStageBuilder {
  final protected val fieldCustomSQL = "customSql"

  override def validateStorage(config: Map[String, String]): Boolean =
    config.get(fieldStorageId).exists(s => drivers.contains(s.toLowerCase))

  override protected def validateRead(config: Map[String, String]): Boolean =
    validateJdbc(config) && config.contains(fieldCustomSQL) &&
      ((!config(fieldCustomSQL).toBoolean && config.contains(fieldSchema) && config.contains(fieldTable)) ||
          (config(fieldCustomSQL).toBoolean && config.contains("option.".concat(JDBCOptions.JDBC_TABLE_NAME))))

  override protected def convert(config: Node): Stage = {
    val (schemaTable, map) = jdbcParams(config)
    val truststorePathOption = if (config.value.contains(fieldCertData)) Some(truststorePath) else None
    new JdbcReadStage(config.id, schemaTable, truststorePathOption, map, getOptions(config.value), config.value(fieldCustomSQL).toBoolean)
  }
}
