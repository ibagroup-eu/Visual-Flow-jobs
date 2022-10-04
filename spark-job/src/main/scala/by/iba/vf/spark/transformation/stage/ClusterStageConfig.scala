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
import by.iba.vf.spark.transformation.stage.ClusterStageConfig._

object ClusterStageConfig {
  val storageId = "cluster"

  val pathFieldName = "path"
  val formatFieldName = "format"

  val useSchemaFieldName = "useSchema"
  val writeModeFieldName = "writeMode"
  val partitionByFieldName = "partitionBy"
}

class ClusterStageConfig(config: Node) {
  val path: String = config.value(pathFieldName)
  val format: String = config.value(formatFieldName)

  val useSchema: Boolean = config.value.get(useSchemaFieldName).exists(x => x.toBoolean)
  val writeMode: Option[String] = config.value.get(writeModeFieldName)
  val partitionBy: Option[Array[String]] = config.value.get(partitionByFieldName).map(x => x.split(',').map(c => c.trim))
}
