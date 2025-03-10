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
package by.iba.vf.spark.transformation.stage.mixins

import by.iba.vf.spark.transformation.config.Node
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

trait IncrementalLoad {
  val fieldIncrementalLoad = "incrementalLoad"
  val fieldIncrementalOffsetKey = "incrementalOffsetKey"
  val fieldIncrementalOffsetValue = "incrementalOffsetValue"

  def getIncrementalLoadParams(config: Map[String, String]): Map[String, String] = {
    val incrementalLoad = config.get(fieldIncrementalLoad)
    val incrementalOffsetKey = config.get(fieldIncrementalOffsetKey)
    val incrementalOffsetValue = config.get(fieldIncrementalOffsetValue)

    var configMap: Map[String, String] = Map()
    configMap += fieldIncrementalLoad -> incrementalLoad.getOrElse("false")
    configMap += fieldIncrementalOffsetKey -> incrementalOffsetKey.getOrElse("")
    configMap += fieldIncrementalOffsetValue -> incrementalOffsetValue.getOrElse("")

    configMap
  }

  def validateIncrementalLoad(config: Map[String, String]): Boolean =
    (
      config.contains(fieldIncrementalLoad) && (
        (config.getOrElse(fieldIncrementalLoad, "false").toBoolean && config.contains(fieldIncrementalOffsetKey)
        && config(fieldIncrementalOffsetKey).nonEmpty)
          || !config.getOrElse(fieldIncrementalLoad, "false").toBoolean)
      ) || !config.contains(fieldIncrementalLoad)

  def applyIncrementalLoadBehavior(df: DataFrame, config: Map[String, String]): DataFrame = {
    if (config.getOrElse(fieldIncrementalLoad, "false").toBoolean)
      df
        .where(
          lit(config.getOrElse(fieldIncrementalOffsetValue, "") == "").or(
            col(config(fieldIncrementalOffsetKey)) > lit(config.getOrElse(fieldIncrementalOffsetValue, "")))
        )
    else
      df
  }

}
