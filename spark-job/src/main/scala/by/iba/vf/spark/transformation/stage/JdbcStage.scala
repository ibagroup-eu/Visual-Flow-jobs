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
package by.iba.vf.spark.transformation.stage

import java.util.Base64
import java.util.UUID

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.exception.TransformationConfigurationException
import by.iba.vf.spark.transformation.utils.TruststoreGenerator
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

trait JdbcStageBuilder extends StageBuilder {
  val truststorePath = "jdbc-source-truststore.jks"
  val drivers: Map[String, String] = Map(
    "db2" -> "com.ibm.db2.jcc.DB2Driver",
    "sqlite" -> "org.sqlite.JDBC",
    "oracle" -> "oracle.jdbc.driver.OracleDriver",
    "mysql" -> "com.mysql.cj.jdbc.Driver",
    "postgresql" -> "org.postgresql.Driver",
    "mssql" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "redshift-jdbc" -> "com.amazon.redshift.jdbc42.Driver",
  )

  val jdbcStorage = "jdbc"
  private val fieldJdbcUrl = "jdbcUrl"
  private val fieldUser = "user"
  private val fieldPassword = "password"
  val fieldCertData = "certData"
  val fieldWriteMode = "writeMode"
  val fieldSchema = "schema"
  val fieldStorage = "storage"
  val fieldTable = "table"
  private val fieldNumPartitions = "numPartitions"
  private val fieldPartitionColumn = "partitionColumn"
  private val fieldLowerBound = "lowerBound"
  private val fieldUpperBound = "upperBound"
  private val fieldFetchSize = "fetchsize"
  private val fieldBatchSize = "batchsize"
  private val fieldSessionInitStatement = "sessionInitStatement"
  private val fieldCreateTableOptions = "createTableOptions"
  private val fieldCreateTableColumnTypes = "createTableColumnTypes"
  private val fieldCustomSchema = "customSchema"
  private val fieldPrepareQuery = "prepareQuery"

  def jdbcParams(config: Node): (String, Map[String, String]) = {
    val jdbcUrl = config.value(fieldJdbcUrl)
    val fullyQualifiedTable = getFullyQualifiedTable(config)
    val user = config.value(fieldUser)
    val password = config.value(fieldPassword)
    val numPartitions = config.value.get(fieldNumPartitions)
    val partitionColumn  = config.value.get(fieldPartitionColumn)
    val lowerBound = config.value.get(fieldLowerBound)
    val upperBound = config.value.get(fieldUpperBound)
    val fetchSize = config.value.get(fieldFetchSize)
    val batchSize = config.value.get(fieldBatchSize)
    val sessionInitStatement = config.value.get(fieldSessionInitStatement)
    val createTableOptions = config.value.get(fieldCreateTableOptions)
    val createTableColumnTypes = config.value.get(fieldCreateTableColumnTypes)
    val customSchema = config.value.get(fieldCustomSchema)
    val prepareQuery = config.value.get(fieldPrepareQuery)
    val driver = drivers
      .find { case (typ, _) => config.value(fieldStorage).equalsIgnoreCase(typ) }
      .map { case (_, className) =>
        Class.forName(className)
        className
      }
      .getOrElse {
        throw new TransformationConfigurationException(s"Driver for url $jdbcUrl not found.")
      }

    val url = config.value.get(fieldCertData) match {
      case Some(certificate) =>
        val password = UUID.randomUUID().toString
        TruststoreGenerator.createTruststoreWithGivenCert(
          password,
          certificate = Base64.getDecoder.decode(certificate),
          certificateType = "X.509",
          truststoreType = "JKS",
          truststorePath = truststorePath
        )
        buildUrl(jdbcUrl, password)
      case _ => jdbcUrl
    }

    var configMap = Map(
      JDBCOptions.JDBC_URL -> url,
      fieldUser -> user,
      fieldPassword -> password,
      JDBCOptions.JDBC_DRIVER_CLASS -> driver
    )

    if (numPartitions.orNull != null) {
      configMap += JDBCOptions.JDBC_NUM_PARTITIONS -> numPartitions.orNull
    }

    if (numPartitions.orNull != null && partitionColumn.orNull != null &&
          lowerBound.orNull != null && upperBound.orNull != null) {
      configMap += JDBCOptions.JDBC_PARTITION_COLUMN -> partitionColumn.orNull
      configMap += JDBCOptions.JDBC_LOWER_BOUND -> lowerBound.orNull
      configMap += JDBCOptions.JDBC_UPPER_BOUND -> upperBound.orNull
    }

    fetchSize.foreach { configMap += JDBCOptions.JDBC_BATCH_FETCH_SIZE -> _ }
    batchSize.foreach { configMap += JDBCOptions.JDBC_BATCH_INSERT_SIZE -> _ }
    sessionInitStatement.foreach { configMap += JDBCOptions.JDBC_SESSION_INIT_STATEMENT -> _ }
    createTableOptions.foreach { configMap += JDBCOptions.JDBC_CREATE_TABLE_OPTIONS -> _ }
    createTableColumnTypes.foreach { configMap += JDBCOptions.JDBC_CREATE_TABLE_COLUMN_TYPES -> _ }
    customSchema.foreach { configMap += JDBCOptions.JDBC_CUSTOM_DATAFRAME_COLUMN_TYPES -> _ }
    prepareQuery.foreach { configMap += JDBCOptions.JDBC_PREPARE_QUERY -> _ }

    (fullyQualifiedTable, configMap)
  }

  def getFullyQualifiedTable(config: Node): String =
    s"${config.value.get(fieldSchema).map(_ + ".").getOrElse("")}${config.value.getOrElse(fieldTable, "")}"

  def buildUrl(jdbcUrl: String, password: String): String =
    s"$jdbcUrl:sslConnection=true;sslTrustStoreLocation=$truststorePath;sslTrustStorePassword=$password;"

  def validateJdbc(config: Map[String, String]): Boolean =
    drivers.keySet.contains(config.get(fieldStorage).orNull.toLowerCase) &&
      config.contains(fieldJdbcUrl) && config.contains(fieldUser) && config.contains(fieldPassword)

}
