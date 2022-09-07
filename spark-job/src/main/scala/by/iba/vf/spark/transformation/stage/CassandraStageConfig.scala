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
import by.iba.vf.spark.transformation.stage.CassandraStageConfig._
import by.iba.vf.spark.transformation.utils.TruststoreGenerator
import net.liftweb.json
import org.apache.spark.sql.SparkSession

import java.util.UUID

class CassandraStageConfig(config: Node) {
  private implicit val Formats: json.DefaultFormats.type = json.DefaultFormats
  val table: String = config.value.getOrElse(tableFieldName, throw new TransformationConfigurationException("Table name was not provided"))
  val keyspace: String = config.value.getOrElse(keyspaceFieldName, throw new TransformationConfigurationException("Keyspace was not provided"))
  val cluster: Option[String] = config.value.get(clusterFieldName)
  val pushdownEnabled: Boolean = config.value.get(pushdownEnabledFieldName).exists(x => x.toBoolean)
  val writeMode: Option[String] = config.value.get(writeModeFieldName)
  val ssl: Boolean = config.value.get("ssl").exists(x => x.toBoolean)
  val certData: Option[String] = config.value.get(certDataFieldName)

  def applyConfig(spark: SparkSession): Unit = {
    allParameters.foreach { case (configField, (parameter, nullable)) =>
      (config.value.get(configField), nullable) match {
        case (Some(value), _) => spark.conf.set(parameter, value)
        case (None, true) => None
        case (None, false) => throw new TransformationConfigurationException(s"mandatory field $configField not found")
      }
    }

    val nodePath = s"/tmp/cassandra-truststore-${config.id}.jks"
    val sslParams = certData match {
      case Some(cert) if ssl =>
        val pass = UUID.randomUUID().toString

        TruststoreGenerator.createTruststoreOnExecutors(
          spark,
          Some((cert, pass, nodePath))
        )

        Map("spark.cassandra.connection.ssl.trustStore.path" -> nodePath,
          "spark.cassandra.connection.ssl.trustStore.password" -> pass)
      case _ => Map.empty
    }
    sslParams.foreach(keyVal => spark.conf.set(keyVal._1, keyVal._2))
  }
}

object CassandraStageConfig {

  val storageId = "cassandra"
  val tableFieldName = "table"
  val keyspaceFieldName = "keyspace"
  val clusterFieldName = "cluster"
  val pushdownEnabledFieldName = "pushdownEnabled"
  val writeModeFieldName = "writeMode"
  val additionalConfigSettingsFieldName = "additionalConfigSettings"
  val certDataFieldName = "certData"

  private val allParameters = Map(
    "host" -> ("spark.cassandra.connection.host", false),
    "port" -> ("spark.cassandra.connection.port", false),
    "ssl" -> ("spark.cassandra.connection.ssl.enabled", false),
    "password" -> ("spark.cassandra.auth.password", false),
    "username" -> ("spark.cassandra.auth.username", false),
  )

  def validateConfig(config: Map[String, String]): Boolean =
    allParameters.forall { case (param, (_, nullable)) =>
      config.contains(param) || nullable
    } && config.contains(tableFieldName) && config.contains(keyspaceFieldName) && config.contains(pushdownEnabledFieldName)


}
