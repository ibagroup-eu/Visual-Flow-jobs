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
import by.iba.vf.spark.transformation.exception.TransformationConfigurationException

private[function] final class JoinStage(
    val configNode: Node,
    joinType: String,
    columnMap: Map[String, Option[String]],
    selectedColumns: Array[String],
    leftDataset: String,
    rightDataset: String
  ) extends Stage {
  override val operation: OperationType.Value = OperationType.JOIN
  override val inputsRequired: Int = 2
  override val builder: StageBuilder = JoinStageBuilder

  override protected def process(input: Map[String, DataFrame])(implicit spark: SparkSession): Option[DataFrame] = {
    Some(join(input(leftDataset), input(rightDataset)))
  }

  private def join(l: DataFrame, r: DataFrame): DataFrame =
    joinType match {
      case "cross" => l.crossJoin(r)
                        .select(selectedColumns.head, selectedColumns.tail: _*)
      case _ =>
        columnMap("columns") match {
          case Some(columns) =>
            val columnsSeq = columns.split(",").map(_.trim)
            l.as("left").join(r.as("right"), columnsSeq, joinType)
              .select(selectedColumns.head, selectedColumns.tail: _*)
          case None =>
            (columnMap("leftColumns"), columnMap("rightColumns")) match {
              case (Some(leftColumns), Some(rightColumns)) =>
                val leftColumnsSeq = leftColumns.split(",").map(_.trim)
                val rightColumnsSeq = rightColumns.split(",").map(_.trim)

                if (leftColumnsSeq.length != rightColumnsSeq.length)
                  throw new TransformationConfigurationException("Not equal number of columns")

                var columns = l(leftColumnsSeq(0)) === r(rightColumnsSeq(0))
                for ((left, right) <- leftColumnsSeq.drop(1) zip rightColumnsSeq.drop(1))
                  columns = columns && l(left) === r(right)

                l.as("left").join(r.as("right"), columns, joinType)
                  .select(selectedColumns.head, selectedColumns.tail: _*)
              case (Some(_), None) =>
                throw new TransformationConfigurationException("rightColumns field not found")
              case (None, Some(_)) =>
                throw new TransformationConfigurationException("leftColumns field not found")
              case (None, None) =>
                throw new TransformationConfigurationException("Column fields not found")
        }
      }
    }
}

object JoinStageBuilder extends StageBuilder {

  private val FieldJoinType = "joinType"
  private val FieldColumns = "columns"
  private val FieldLeftColumns = "leftColumns"
  private val FieldRightColumns = "rightColumns"
  private val FieldLeftDataset = "leftDataset"
  private val FieldRightDataset = "rightDataset"
  private val FieldSelectedLeftColumns = "selectedLeftColumns"
  private val FieldSelectedRightColumns = "selectedRightColumns"

  override protected def validate(config: Map[String, String]): Boolean =
    (config.get(fieldOperation).contains(OperationType.JOIN.toString) && config.contains(FieldJoinType) && (config
      .contains(FieldColumns) || (config.contains(FieldLeftColumns) && config.contains(FieldRightColumns))) &&
      config.contains(FieldLeftDataset) && config.contains(FieldRightDataset)) ||
      (config.get(fieldOperation).contains(OperationType.JOIN.toString) && config.contains(FieldJoinType) &&
        config.contains(FieldLeftDataset) && config.contains(FieldRightDataset))

  override protected def convert(config: Node): Stage = {
    val id = config.id
    val joinType = config.value(FieldJoinType)
    val leftDataset = config.value(FieldLeftDataset)
    val rightDataset = config.value(FieldRightDataset)
    val columns = config.value.get(FieldColumns)
    val leftColumns = config.value.get(FieldLeftColumns)
    val rightColumns = config.value.get(FieldRightColumns)
    val selectedLeftColumns = config.value.getOrElse(FieldSelectedLeftColumns, "*")
      .split(",")
      .map(_.trim)
      .map(col => s"left.$col")
    val selectedRightColumns = config.value.getOrElse(FieldSelectedRightColumns, "*")
      .split(",")
      .map(_.trim)
      .map(col => s"right.$col")

    val columnMap = Map("columns" -> columns, "leftColumns" -> leftColumns, "rightColumns" -> rightColumns)
    val selectedColumns = selectedLeftColumns.union(selectedRightColumns)
    new JoinStage(config, joinType, columnMap, selectedColumns, leftDataset, rightDataset)
  }
}
