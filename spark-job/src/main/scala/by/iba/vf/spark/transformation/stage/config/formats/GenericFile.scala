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

trait GenericFile {
  private val fieldPathGlobFilter = "pathGlobFilter"
  private val fieldRecursiveFileLookup = "recursiveFileLookup"
  private val fieldModifiedBefore = "modifiedBefore"
  private val fieldModifiedAfter = "modifiedAfter"
  private val fieldCompression = "compression"

  def buildConnectionOptions(config: Node): Map[String, String] = {
    val pathGlobFilter = config.value.get(fieldPathGlobFilter)
    val recursiveFileLookup = config.value.get(fieldRecursiveFileLookup)
    val modifiedBefore = config.value.get(fieldModifiedBefore)
    val modifiedAfter = config.value.get(fieldModifiedAfter)
    val compression = config.value.get(fieldCompression)
    var configMap = Map[String, String]()

    pathGlobFilter.foreach { configMap += "pathGlobFilter" -> _ }
    recursiveFileLookup.foreach { configMap += "recursiveFileLookup" -> _ }
    modifiedBefore.foreach { configMap += "modifiedBefore" -> _ }
    modifiedAfter.foreach { configMap += "modifiedAfter" -> _ }
    compression.foreach { configMap += "compression" -> _ }

    configMap
  }

}
