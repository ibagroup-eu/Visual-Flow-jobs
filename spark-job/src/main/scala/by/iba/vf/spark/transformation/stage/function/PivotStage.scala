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
import by.iba.vf.spark.transformation.exception.TransformationConfigurationException
import by.iba.vf.spark.transformation.stage.OperationType
import by.iba.vf.spark.transformation.stage.Stage
import by.iba.vf.spark.transformation.stage.StageBuilder
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

private[function] final class PivotStage(val id: String, operationType: String, options: Map[String, String]) extends Stage {
  override val operation: OperationType.Value = OperationType.PIVOT
  override val inputsRequired: Int = 1
  override val builder: StageBuilder = PivotStageBuilder

  override protected def process(input: Map[String, DataFrame])(implicit spark: SparkSession): Option[DataFrame] =
    input.values.headOption.map(pivot)

  def makePivotTable(df: DataFrame, pivotOptions: Map[String, String]): DataFrame = {
    if (pivotOptions.contains("pivotValues"))
      df.groupBy(pivotOptions("groupBy").split(",").map(column => col(column.trim)): _*)
        .pivot(col(pivotOptions("pivotColumn")), pivotOptions("pivotValues").split(",").map(value => value.trim))
        .agg(expr(pivotOptions("aggregation")))
    else
      df.groupBy(pivotOptions("groupBy").split(",").map(column => col(column.trim)): _*)
        .pivot(col(pivotOptions("pivotColumn")))
        .agg(expr(pivotOptions("aggregation")))
  }

  def makeUnpivotTable(df: DataFrame, unpivotOptions: Map[String, String]): DataFrame = {
    val unchangedColumns = unpivotOptions("unchangedColumns").split(",").map(column => column.trim)
    val unpivotColumns = unpivotOptions("unpivotColumns").split(",").map(column => column.trim)
    val unpivotNames = unpivotOptions("unpivotNames").split(",").map(name => name.trim)
    val unpivotStack =
      s"stack(${unpivotColumns.length}, " +
      s"${unpivotColumns.map(column => s"'$column', $column").mkString(", ")}) " +
      s"as (${unpivotNames.mkString(", ")})"

    val columns = unchangedColumns :+ unpivotStack
    df.selectExpr(columns: _*).where(s"${unpivotNames(1)} is not null")
  }

  def pivot(df: DataFrame): DataFrame = {
    operationType match {
      case "pivot" => makePivotTable(df, options)
      case "unpivot" => makeUnpivotTable(df, options)
      case _ =>
        throw new TransformationConfigurationException(s"Operation $operationType does not exist")
    }
  }
}

object PivotStageBuilder extends StageBuilder {
  private val FieldOperationType = "operationType"

  override protected def validate(config: Map[String, String]): Boolean =
    config.get(fieldOperation).contains(OperationType.PIVOT.toString) && config.contains(FieldOperationType)

  override protected def convert(config: Node): Stage = {
    val operationType = config.value(FieldOperationType)

    new PivotStage(config.id, operationType, getOptions(config.value))
  }
}
