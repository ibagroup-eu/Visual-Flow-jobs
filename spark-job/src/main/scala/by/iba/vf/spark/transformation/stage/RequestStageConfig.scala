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
import by.iba.vf.spark.transformation.stage.RequestStageConfig._

object RequestStageConfig {
  val storageId = "request"

  val hostFieldName: String = "host"
  val methodFieldName: String = "method"
  val paramsFieldName: String = "params"
  val headersFieldName: String = "headers"
}

class RequestStageConfig(config: Node) {
  val host: String = config.value(hostFieldName)
  val method: String = config.value(methodFieldName)
  val params: Option[String] = config.value.get(paramsFieldName)
  val headers: Option[String] = config.value.get(headersFieldName)

  def makeMap(arg: String): Map[String, String] = {
    arg.split(";").map(_.split(":")).map {case Array(k, v) => (k, v)}.toMap
  }
}
