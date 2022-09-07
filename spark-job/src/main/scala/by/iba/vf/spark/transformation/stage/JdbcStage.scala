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
  val drivers = Map(
    "db2" -> "com.ibm.db2.jcc.DB2Driver",
    "sqlite" -> "org.sqlite.JDBC",
    "oracle" -> "oracle.jdbc.driver.OracleDriver",
    "mysql" -> "com.mysql.cj.jdbc.Driver",
    "postgresql" -> "org.postgresql.Driver",
    "mssql" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "redshift-jdbc" -> "com.amazon.redshift.jdbc42.Driver",
  )

  val jdbcStorage = "jdbc"
  val fieldJdbcUrl = "jdbcUrl"
  val fieldUser = "user"
  val fieldPassword = "password"
  val fieldCertData = "certData"
  val fieldWriteMode = "writeMode"
  val fieldSchema = "schema"
  val fieldStorage = "storage"
  val fieldTable = "table"

  def jdbcParams(config: Node): (String, Map[String, String]) = {
    val jdbcUrl = config.value(fieldJdbcUrl)
    val schemaTable = s"${config.value.get(fieldSchema).map(_ + ".").getOrElse("")}${config.value.getOrElse(fieldTable, "")}"
    val user = config.value(fieldUser)
    val password = config.value(fieldPassword)
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
        s"$jdbcUrl:sslConnection=true;sslTrustStoreLocation=$truststorePath;sslTrustStorePassword=$password;"
      case _ => jdbcUrl
    }

    val configMap = Map(
      JDBCOptions.JDBC_URL -> url,
      fieldUser -> user,
      fieldPassword -> password,
      JDBCOptions.JDBC_DRIVER_CLASS -> driver
    )

    (schemaTable, configMap)
  }

  def validateJdbc(config: Map[String, String]): Boolean =
    drivers.keySet.contains(config.get(fieldStorage).orNull.toLowerCase) &&
      config.contains(fieldJdbcUrl) && config.contains(fieldUser) && config.contains(fieldPassword)
}
