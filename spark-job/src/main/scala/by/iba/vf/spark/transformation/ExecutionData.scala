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
package by.iba.vf.spark.transformation

import org.apache.spark.sql.DataFrame

import scala.collection.mutable

object StagesState {
  var stagesToRun: Seq[String] = Seq[String]()
}

class ExecutionData(var dependencies: Map[String, Seq[String]]) {
  private val stageResults = mutable.Map[String, DataFrame]()
  var stageStatus: Map[String, StageStatus.Value] = Map[String, StageStatus.Value]()
  private val failedStatuses = Seq(StageStatus.FAILED, StageStatus.UPSTREAM_FAILED)
  private var currentLayer = 0

  def getCurrentLayer: Int = currentLayer

  def updateDependencies(dependencies: Map[String, Seq[String]]): Unit =
    this.dependencies = dependencies

  def getStageInputData(id: String): Map[String, DataFrame] =
    dependencies.getOrElse(id, Nil).map(key => key -> stageResults.apply(key)).toMap

  def saveStageResults(result: Map[String, DataFrame]): Unit = {
    stageResults ++= result
    currentLayer += 1
  }

  def saveStageStatus(status: (String, StageStatus.Value)): Unit =
    stageStatus += status

  def isUpstreamFailedAny(upstreamStageIds: Seq[String]): Boolean =
    upstreamStageIds.exists(k => failedStatuses.contains(stageStatus(k)))
}
