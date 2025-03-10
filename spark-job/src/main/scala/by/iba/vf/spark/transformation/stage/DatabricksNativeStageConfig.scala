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
import by.iba.vf.spark.transformation.stage.DatabricksNativeStageConfig._
import by.iba.vf.spark.transformation.stage.config.formats.Delta
import by.iba.vf.spark.transformation.utils.ConnectionUtils

object DatabricksNativeStageConfig {
  val storageId = "databricks"

  val fieldStorage = "storage"
  val fieldCatalog = "catalog"
  val fieldSchema = "schema"
  val fieldObjectType = "objectType"
  val fieldTable = "table"
  val fieldVolume = "volume"
  val fieldFormat = "format"
  val fieldPath = "volumePath"
  val fieldPartitionBy = "partitionBy"
  val fieldWriteMode = "writeMode"
}

class DatabricksNativeStageConfig(config: Node) {
  val catalog: Option[String] = config.value.get(fieldCatalog)
  val schema: Option[String] = config.value.get(fieldSchema)
  val objectType: Option[String] = config.value.get(fieldObjectType)
  val table: Option[String] = config.value.get(fieldTable)
  val volume: Option[String] = config.value.get(fieldVolume)
  val format: Option[String] = config.value.get(fieldFormat)
  val path: Option[String] = config.value.get(fieldPath)
  val saveMode: Option[String] = config.value.get(fieldWriteMode)

  val options: Map[String, String] = objectType.map {
    case "table" => Delta.buildConnectionOptions(config)
    case "volume" => ConnectionUtils.buildFormatOptions(config, format)
  }.getOrElse(Map[String, String]())

  val partitionBy: Array[String] = ConnectionUtils.getPartitions(config, fieldPartitionBy)

  def getFullyQualifiedTable: String =
    s"${catalog.map(_ + ".").getOrElse("")}${schema.map(_ + ".").getOrElse("")}${table.getOrElse("")}"

  def getFullyQualifiedObject: String =
    s"/Volumes/${catalog.map(_ + "/").getOrElse("")}${schema.map(_ + "/").getOrElse("")}${volume.map(_ + "/").getOrElse("")}${path.getOrElse("")}"

}
