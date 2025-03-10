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
package by.iba.vf.spark.transformation.stage.config.objectstores

import by.iba.vf.spark.transformation.config.Node
import org.apache.spark.sql.{DataFrameWriter, Row, SparkSession}

abstract class BaseStorageConfig(config: Node) extends ObjectStorageConfig {
  final val format: Option[String] = config.value.get(fieldFormat)
  final val saveMode: Option[String] = config.value.get(fieldWriteMode)
  final val partitionBy: Option[Array[String]] = config.value.get(fieldPartitionBy).map(x => x.split(',').map(c => c.trim))
  final protected val id: String = config.id
  final val useSchema: Boolean = config.value.get(fieldUseSchema).exists(x => x.toBoolean)

  def setConfig(spark: SparkSession): Unit

  def connectPath: String

  def addPartitions(dfWriter: DataFrameWriter[Row]): DataFrameWriter[Row] = {
    partitionBy.map(columns => dfWriter.partitionBy(columns: _*)).getOrElse(dfWriter)
  }

}
