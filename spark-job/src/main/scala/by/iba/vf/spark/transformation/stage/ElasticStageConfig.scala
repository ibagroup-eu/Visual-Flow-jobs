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

import java.util.UUID

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.exception.TransformationConfigurationException
import by.iba.vf.spark.transformation.stage.ElasticStageConfig.allParameters

class ElasticStageConfig(config: Node) {
  def elasticParams: (Map[String, String], Map[String, String], Option[(String, String, String)]) = {
    val conf = allParameters.flatMap { case (configField, (parameter, nullable)) =>
      (config.value.get(configField), nullable) match {
        case (Some(value), _) => Some(parameter -> value)
        case (None, true) => None
        case (None, false) => throw new TransformationConfigurationException(s"mandatory field $configField not found")
      }
    }

    val elasticConf = conf.filterKeys(_.startsWith("es.")) + ("es.nodes.wan.only" -> "true")
    val jobConf = conf.filterKeys(!_.startsWith("es."))


    val nodePath = s"/tmp/elastic-truststore-${config.id}.jks"
    val (certConf, certDataPass) = jobConf.get("certData") match {
      case Some(certData) =>
        val pass = UUID.randomUUID().toString
        (Map(
          "es.net.ssl.truststore.location" -> s"file://$nodePath",
          "es.net.ssl.truststore.pass" -> pass),
          Some((certData, pass, nodePath))
        )
      case _ => (Map.empty, None)
    }

    (jobConf, elasticConf ++ certConf, certDataPass)
  }
}

object ElasticStageConfig {
  val storageId = "elastic"
  private[stage] val index = "index"
  private[stage] val writeMode = "writeMode"
  private[stage] val truststorePath = "truststorePath"
  private[stage] val certData = "certData"

  private val allParameters = Map(
    "nodes" -> ("es.nodes", false),
    "ssl" -> ("es.net.ssl", false),
    "port" -> ("es.port", false),
    "password" -> ("es.net.http.auth.pass", false),
    "user" -> ("es.net.http.auth.user", false),
    index -> (index, false),
    truststorePath -> (truststorePath, true),
    certData -> (certData, true),
    writeMode -> (writeMode, true)
  )

  def validateElastic(config: Map[String, String]): Boolean =
    allParameters.forall { case (param, (_, nullable)) =>
      config.contains(param) || nullable
    }
}
