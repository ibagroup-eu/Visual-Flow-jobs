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

object WholeBinary extends GenericFile {
  private val fieldBinaryFormat = "binaryFormat"
  private val fieldOutputContentColumn = "outputContentColumn"
  private val fieldOutputPathColumn = "outputPathColumn"

  override def buildConnectionOptions(config: Node): Map[String, String] = {
    val binaryFormat: Option[String] = config.value.get(fieldBinaryFormat)
    val outputContentColumn: Option[String] = config.value.get(fieldOutputContentColumn)
    val outputPathColumn: Option[String] = config.value.get(fieldOutputPathColumn)

    var configMap = super.buildConnectionOptions(config)
    binaryFormat.foreach { value => configMap += "binaryFormat" -> s"$value".toLowerCase }
    binaryFormat.foreach { value => configMap += "pathGlobFilter" -> s"*.$value" }
    outputContentColumn.foreach { configMap += "outputContentColumn" -> _ }
    outputPathColumn.foreach { configMap += "outputPathColumn" -> _ }

    configMap
  }

}
