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

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.stage.ClickhouseStageConfig._

object ClickhouseStageConfig {
  val storageId = "clickhouse"

  val hostFieldName: String = "host"
  val portFieldName: String = "port"
  val databaseFieldName: String = "database"
  val userFieldName: String = "user"
  val passwordFieldName: String = "password"

  val customSqlFieldName: String = "customSql"
  val schemaFieldName: String = "schema"
  val tableFieldName: String = "table"
  val queryFieldName = "query"

  val writeModeFieldName: String = "writeMode"
}

class ClickhouseStageConfig(config: Node) {
  val host: String = config.value(hostFieldName)
  val port: String = config.value(portFieldName)
  val database: String = config.value(databaseFieldName)
  val user: String = config.value(userFieldName)
  val password: String = config.value(passwordFieldName)

  val customSql: Boolean = config.value.get(customSqlFieldName).exists(x => x.toBoolean)
  val schema: Option[String] = config.value.get(schemaFieldName)
  val table: Option[String] = config.value.get(tableFieldName)
  val query: Option[String] = config.value.get(queryFieldName)

  val write_mode: Option[String] = config.value.get(writeModeFieldName)

  var parameter: Map[String, String] = Map(
    "url" -> s"jdbc:clickhouse://$host:$port/$database",
    "user" -> user,
    "password" -> password,
    "driver" -> "com.clickhouse.jdbc.ClickHouseDriver"
  )

  if (!customSql)
    parameter += ("dbtable" -> s"${schema.orNull}.${table.orNull}")
  else
    parameter += ("query" -> query.orNull)
}
