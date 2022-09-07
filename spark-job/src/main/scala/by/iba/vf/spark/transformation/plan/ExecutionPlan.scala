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
package by.iba.vf.spark.transformation.plan

import by.iba.vf.spark.transformation.ResultLogger
import by.iba.vf.spark.transformation.stage.Stage

import scala.collection.mutable.ArrayBuffer

final case class ExecutionPlan(plan: Seq[Set[Stage]], dependencyMap: Map[String, Seq[String]]) extends ResultLogger {

  def printExecutionPlan(): Unit = {
    val widthPerLayer: Array[Int] = plan.map(_.map(_.id).maxBy(_.length).length).toArray

    val maxNumOfRows = plan.maxBy(_.size).size
    val planAsArray = plan.toArray.map(_.toArray)
    val spaceStr = " "

    val results = ArrayBuffer[String]()
    for (i <- 0 until maxNumOfRows) {
      val sb = new StringBuilder()
      for (j <- planAsArray.indices) {
        if (planAsArray(j).length >= maxNumOfRows - i) {
          val str = s" (${planAsArray(j)(maxNumOfRows - i - 1).id}) "
          sb.append(str)
          if (str.length - 4 < widthPerLayer(j)) {
            sb.append(spaceStr * (widthPerLayer(j) - str.length + 4))
          }
        } else {
          sb.append(spaceStr * (4 + widthPerLayer(j)))
        }
        sb.append(x = '|')
      }
      results += sb.toString
    }
    logger.info("{}", System.lineSeparator() + results.mkString(System.lineSeparator()))
  }
}
