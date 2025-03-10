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

object SnowflakeStageConfig {
  val storageId = "snowflake"

  val sfURLFieldName: String = "sfURL"
  val sfUserFieldName: String = "sfUser"
  val sfPasswordFieldName: String = "sfPassword"
  val pemPrivateKeyFieldName: String = "pem_private_key"
  val sfAuthenticatorFieldName: String = "sfAuthenticator"
  val sfTokenFieldName: String = "sfToken"
  val sfDatabaseFieldName: String = "sfDatabase"
  val sfSchemaFieldName: String = "sfSchema"
  val sfWarehouseFieldName: String = "sfWarehouse"
  val sfRoleFieldName: String = "sfRole"
  val sfQueryFieldName: String = "query"
  val sfTableFieldName: String = "dbtable"

}

class SnowflakeStageConfig(config: Node) {
  val sfURL: String = config.value(SnowflakeStageConfig.sfURLFieldName)
  val sfUser: String = config.value(SnowflakeStageConfig.sfUserFieldName)
  val sfPassword: Option[String] = config.value.get(SnowflakeStageConfig.sfPasswordFieldName)
  val pemPrivateKey: Option[String] = config.value.get(SnowflakeStageConfig.pemPrivateKeyFieldName)
  val sfToken: Option[String] = config.value.get(SnowflakeStageConfig.sfTokenFieldName)
  val sfDatabase: String = config.value(SnowflakeStageConfig.sfDatabaseFieldName)
  val sfSchema: String = config.value(SnowflakeStageConfig.sfSchemaFieldName)
  val sfWarehouse: Option[String] = config.value.get(SnowflakeStageConfig.sfWarehouseFieldName)
  val sfRole: Option[String] = config.value.get(SnowflakeStageConfig.sfRoleFieldName)
  val query: Option[String] = config.value.get(SnowflakeStageConfig.sfQueryFieldName)
  val dbtable: Option[String] = config.value.get(SnowflakeStageConfig.sfTableFieldName)

  var parameter: Map[String, String] = Map(
    "sfURL" -> sfURL,
    "sfUser" -> sfUser,
    "sfDatabase" -> sfDatabase,
    "sfSchema" -> sfSchema,
    "sfWarehouse" -> sfWarehouse.getOrElse(""),
    "sfRole" -> sfRole.getOrElse("")
  )

  (sfPassword, pemPrivateKey, sfToken) match {
    case (Some(password), _, _) =>
      parameter += ("sfPassword" -> password)
    case (_, Some(key), _) =>
      parameter += ("pemPrivateKey" -> key)
    case (_, _, Some(token)) =>
      parameter += ("sfAuthenticator" -> "oauth")
      parameter += ("sfToken" -> token)
    case _ =>
      throw new IllegalArgumentException(
        "Invalid authentication parameters."
      )
  }

  (query, dbtable) match {
    case (Some(query), _) =>
      parameter += ("query" -> query)
    case (_, Some(dbTable)) =>
      parameter += ("dbtable" -> dbTable)
    case _ =>
      throw new IllegalArgumentException("Neither query nor dbtable was provided.")
  }

}
