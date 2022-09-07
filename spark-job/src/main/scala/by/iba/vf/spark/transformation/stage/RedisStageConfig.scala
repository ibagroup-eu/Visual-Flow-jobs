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
import by.iba.vf.spark.transformation.exception.TransformationConfigurationException
import by.iba.vf.spark.transformation.stage.RedisStageConfig._

class RedisStageConfig(config: Node) {
  val table: Option[String] = config.value.get(tableFieldName)
  val saveMode: Option[String] = config.value.get(writeModeFieldName)
  val readMode: Option[String] = config.value.get(readModeFieldName)
  val keyColumn: Option[String] = config.value.get(keyColumnFieldName)
  val model: Option[String] = config.value.get(modelFieldName)
  val keysPattern: Option[String] = config.value.get(keysPatternFieldName)
  val ttl: Option[String] = config.value.get(ttlFieldName)

  def provideConnectionOptions: Map[String, String] = {
    var configMap: Map[String, String] = Map.empty
    allParameters.foreach { case (configField, (alias, nullable)) =>
      val optionName = alias match {
        case a: String => a
        case None => configField
      }
      (config.value.get(configField), nullable) match {
        case (Some(value), _) => configMap += (optionName -> value)
        case (None, true) => None
        case (None, false) => throw new TransformationConfigurationException(s"mandatory field $configField not found")
      }
    }
    configMap
  }
}

object RedisStageConfig {

  val storageId = "redis"
  val tableFieldName = "table"
  val keyColumnFieldName = "keyColumn"
  val modelFieldName = "model"
  val writeModeFieldName = "writeMode"
  val readModeFieldName = "readMode"
  val ttlFieldName = "ttl"
  val keysPatternFieldName = "keysPattern"
  val sslFieldName = "ssl"
  val tableActualName = "table"
  val keyColumnActualName = "key.column"
  val keysPatternActualName = "keys.pattern"
  val ttlActualName = "ttl"
  val modelActualName = "model"
  val readModePattern = "pattern"
  val readModeKey = "key"

  private val allParameters = Map(
    "host" -> (None, false),
    "port" -> (None, false),
    sslFieldName -> (None, false),
    "password" -> ("auth", false),
  )

  def validateConfig(config: Map[String, String]): Boolean =
    allParameters.forall { case (param, (_, nullable)) =>
      config.contains(param) || nullable
    }


}
