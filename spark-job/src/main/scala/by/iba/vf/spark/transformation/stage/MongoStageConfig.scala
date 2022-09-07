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
import by.iba.vf.spark.transformation.stage.MongoStageConfig.allParameters

class MongoStageConfig(config: Node) {
  def mongoParams: Map[String, String] = {
    val conf = allParameters.flatMap { case (configField, (parameter, nullable)) =>
      (config.value.get(configField), nullable) match {
        case (Some(value), _) => Some(parameter -> value)
        case (None, true) => None
        case (None, false) => throw new TransformationConfigurationException(s"mandatory field $configField not found")
      }
    }

    val confUser = conf("user")
    val confPassword = conf("password")
    val confHost = conf("host")
    val confPort = conf("port")
    val confDatabase = conf("database")
    val confCollection = conf("collection")
    val confSsl = conf("ssl")

    val uri = Map(
      "uri" -> s"mongodb://$confUser:$confPassword@$confHost:$confPort/$confDatabase.$confCollection?ssl=$confSsl"
    )
    
    uri
  }
}

object MongoStageConfig {
    val storageId = "mongo"
    private[stage] val database = "database"
    private[stage] val collection = "collection"
    private[stage] val writeMode = "writeMode"
    private[stage] val host = "host"
    private[stage] val port = "port"
    private[stage] val user = "user"
    private[stage] val password = "password"
    private[stage] val ssl = "ssl"

    private val allParameters = Map(
        database -> (database, false),
        collection -> (collection, false),
        writeMode -> (writeMode, true),
        host -> (host, false),
        port -> (port, false),
        user -> (user, false), 
        password -> (password, false),
        ssl -> (ssl, false)
    )

    def validateMongo(config: Map[String, String]): Boolean = {
      allParameters.forall { case (param, (_, nullable)) =>
        config.contains(param) || nullable
      }
    }

}