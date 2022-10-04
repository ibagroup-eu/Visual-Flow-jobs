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
    val id: String,
    joinType: String,
    columns: Option[String],
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
    if (joinType == "cross") l.crossJoin(r)
    else {
      val columnsSeq = columns match {
        case Some(str) => str.split(",").map(_.trim)
        case None => throw new TransformationConfigurationException("columns field not found")
      }
      l.as("left").join(r.as("right"), columnsSeq, joinType)
    }
}

object JoinStageBuilder extends StageBuilder {

  private val FieldJoinType = "joinType"
  private val FieldColumns = "columns"
  private val FieldLeftDataset = "leftDataset"
  private val FieldRightDataset = "rightDataset"

  override protected def validate(config: Map[String, String]): Boolean =
    (config.get(fieldOperation).contains(OperationType.JOIN.toString) && config.contains(FieldJoinType) && config
      .contains(FieldColumns) && config.contains(FieldLeftDataset) && config.contains(FieldRightDataset)) ||
    (config.get(fieldOperation).contains(OperationType.JOIN.toString) && config.contains(FieldJoinType) &&
      config.contains(FieldLeftDataset) && config.contains(FieldRightDataset))

  override protected def convert(config: Node): Stage = {
    val id = config.id
    val joinType = config.value(FieldJoinType)
    val leftDataset = config.value(FieldLeftDataset)
    val rightDataset = config.value(FieldRightDataset)
    val fields = config.value.get(FieldColumns)

    new JoinStage(id, joinType, fields, leftDataset, rightDataset)
  }
}
