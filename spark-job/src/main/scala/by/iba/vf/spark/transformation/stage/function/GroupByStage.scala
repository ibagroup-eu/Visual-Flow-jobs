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
package by.iba.vf.spark.transformation.stage.function

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.stage.OperationType
import by.iba.vf.spark.transformation.stage.Stage
import by.iba.vf.spark.transformation.stage.StageBuilder
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions

private[function] final class GroupByStage(
    val configNode: Node,
    groupingCols: Array[String],
    colFun: Array[(String, String)],
    oldNewNames: Map[String, String],
    dropGroupingColumns: Boolean
) extends Stage {
  override val operation: OperationType.Value = OperationType.GROUP
  override val inputsRequired: Int = 1
  override val builder: StageBuilder = GroupByStageBuilder

  override protected def process(input: Map[String, DataFrame])(implicit spark: SparkSession): Option[DataFrame] =
    input.values.headOption.map(groupBy)

  def groupBy(df: DataFrame): DataFrame = {
    val aggDF = df.groupBy(groupingCols.map(functions.col): _*).agg(colFun.head, colFun.tail: _*)

    val result = oldNewNames.foldLeft(aggDF){
      case (tempDF, (oldName, newName)) => tempDF.withColumnRenamed(oldName, newName)
    }

    if (dropGroupingColumns) {
      result.drop(groupingCols: _*)
    } else {
      result
    }
  }
}

object GroupByStageBuilder extends StageBuilder {
  private val FieldGroupingColumns = "groupingColumns"
  private val FieldGroupingCriteria = "groupingCriteria"
  private val FieldDropGroupingColumns = "dropGroupingColumns"

  override protected def validate(config: Map[String, String]): Boolean =
    config.get(fieldOperation).contains(OperationType.GROUP.toString) && config
      .contains(FieldGroupingColumns) && config.contains(FieldGroupingCriteria)

  override protected def convert(config: Node): Stage = {
    val comma = ","
    val id = config.id
    val groupingCols = config.value(FieldGroupingColumns).split(comma).map(_.trim)
    val aggregatingCriteria = config
      .value(FieldGroupingCriteria)
      .split(comma)
      .map { colFun =>
        val Array(col, fun, newCol) = if (colFun.count(_ == ':') == 2) {
          colFun.split(":", 3)
        } else {
          s"$colFun:".split(":", 3)
        }
        col.trim -> fun.trim -> newCol.trim
      }

    val renamingCriteria = aggregatingCriteria.foldLeft(Map.empty[String, String]) { case (map, ((col, fun), newCol)) =>
      if (newCol.nonEmpty) {
        map + (s"$fun($col)" -> newCol)
      } else {
        map
      }
    }

    val groupingCriteria = aggregatingCriteria.map { case ((col, fun), _) =>
      (col, fun)
    }

    val dropGroupingColumns = config.value.getOrElse(FieldDropGroupingColumns, "false")
    new GroupByStage(config, groupingCols, groupingCriteria, renamingCriteria, dropGroupingColumns.toBoolean)
  }
}
