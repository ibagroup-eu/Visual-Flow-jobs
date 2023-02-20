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
import by.iba.vf.spark.transformation.stage.{OperationType, Stage, StageBuilder}
import org.apache.spark.ml.feature.Imputer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, min}

private[function] final class HandleNullStage(val id: String, mode: HandleNullType.Value, options: Map[String, String]) extends Stage {
  override val operation: OperationType.Value = OperationType.HANDLE_NULL
  override val inputsRequired: Int = 1
  override val builder: StageBuilder = HandleNullStageBuilder

  override protected def process(input: Map[String, DataFrame])(implicit spark: SparkSession): Option[DataFrame] =
    input.values.headOption.map(handleNull)

  def handleNull(df: DataFrame): DataFrame = {
    mode match {
      case HandleNullType.DROP => drop(df)
      case HandleNullType.FILL => fill(df)
    }
  }

  def fill(df: DataFrame): DataFrame =
    options("fillValueType") match {
      case "custom" =>
        if (options.contains("fillColumns")) {
          val fillColumns = options("fillColumns").split(",")
          val fillMap = (fillColumns zip options("fillValues").split(",")).toMap
          df.na.fill(fillMap)
        } else {
          df.na.fill(options("fillValues"))
        }
      case "agg" =>
        new Imputer()
          .setInputCols(options("fillColumns").split(","))
          .setOutputCols(options("fillColumns").split(","))
          .setStrategy(options("fillStrategy"))
          .fit(df).transform(df)
    }

  def drop(df: DataFrame): DataFrame = {
    options("dropType") match {
      case "column" =>
        options("dropChoice") match {
          case "all" =>
            val minColumns = df.columns.map(name => min(name).alias(name))
            val minValuesRow = df.select(minColumns: _*).first

            val nullColumnNames = df.columns.zipWithIndex.filter({case (_, index) => minValuesRow.isNullAt(index)}).map(_._1)
            df.drop(nullColumnNames: _*)
          case "names" =>
            val userColumns = options("dropColumns").split(',')
            val minColumns = df.columns.map(name => min(name).alias(name))
            val minValuesRow = df.select(minColumns: _*).first

            val nullColumnNames = df.columns.zipWithIndex.filter({ case (_, index) => minValuesRow.isNullAt(index) }).map(_._1)

            if (nullColumnNames.isEmpty || (!nullColumnNames.isEmpty && (userColumns.deep != nullColumnNames.deep)))
              throw new TransformationConfigurationException(s"Column is not NULL: ${userColumns.mkString(",")}")

            df.drop(nullColumnNames: _*)
        }
      case "row" =>
        options("dropChoice") match {
          case "all" => df.na.drop("all")
          case "any" => df.na.drop("any")
          case "names" =>
            val userColumns = options("dropColumns").split(',')
            val filterCond = userColumns.map(x=>col(x).isNotNull).reduce(_ && _)
            df.filter(filterCond)
        }
    }
  }
}

object HandleNullStageBuilder extends StageBuilder {
  private val fieldMode = "mode"

  override protected def validate(config: Map[String, String]): Boolean =
    config.get(fieldOperation).contains(OperationType.HANDLE_NULL.toString) && config.contains(fieldMode)

  override protected def convert(config: Node): Stage = {
    val mode: HandleNullType.Value = HandleNullType.withName(config.value(fieldMode).toUpperCase)
    new HandleNullStage(config.id, mode, getOptions(config.value))
  }
}

object HandleNullType extends Enumeration {
  val DROP, FILL = Value
}
