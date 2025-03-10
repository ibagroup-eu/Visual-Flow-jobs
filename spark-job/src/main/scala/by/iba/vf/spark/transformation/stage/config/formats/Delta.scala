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
package by.iba.vf.spark.transformation.stage.config.formats

import by.iba.vf.spark.transformation.config.Node

object Delta {
  private val fieldMergeSchema = "mergeSchema"
  private val fieldOverwriteSchema = "overwriteSchema"
  private val fieldTimestampAsOf = "timestampAsOf"
  private val fieldVersionAsOf = "versionAsOf"
  private val fieldReplaceWhere = "replaceWhere"
  private val fieldMaxRecordsPerFile = "maxRecordsPerFile"

  def buildConnectionOptions(config: Node): Map[String, String] = {
    val mergeSchema = config.value.get(fieldMergeSchema)
    val overwriteSchema = config.value.get(fieldOverwriteSchema)
    val timestampAsOf = config.value.get(fieldTimestampAsOf)
    val versionAsOf = config.value.get(fieldVersionAsOf)
    val replaceWhere = config.value.get(fieldReplaceWhere)
    val maxRecordsPerFile = config.value.get(fieldMaxRecordsPerFile)
    var configMap = Map[String, String]()

    mergeSchema.foreach { configMap += "mergeSchema" -> _ }
    overwriteSchema.foreach { configMap += "overwriteSchema" -> _ }
    timestampAsOf.foreach { configMap += "timestampAsOf" -> _ }
    versionAsOf.foreach { configMap += "versionAsOf" -> _ }
    replaceWhere.foreach { configMap += "replaceWhere" -> _ }
    maxRecordsPerFile.foreach { configMap += "maxRecordsPerFile" -> _ }

    configMap
  }

}
