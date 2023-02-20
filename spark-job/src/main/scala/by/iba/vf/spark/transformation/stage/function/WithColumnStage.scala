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
import org.apache.spark.sql.functions.{col, expr, lit}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

private[function] final class WithColumnStage(val id: String, column: String,
                                              operationType: String, options: Map[String, String]) extends Stage {
  override val operation: OperationType.Value = OperationType.WITH_COLUMN
  override val inputsRequired: Int = 1
  override val builder: StageBuilder = WithColumnStageBuilder

  override protected def process(input: Map[String, DataFrame])(implicit spark: SparkSession): Option[DataFrame] =
    input.values.headOption.map(useWithColumn)

  def deriveColumn(df: DataFrame, expression: String): DataFrame = {
    df.withColumn(column, expr(expression))
  }

  def addConstant(df: DataFrame, constant: String): DataFrame = {
    df.withColumn(column, lit(constant))
  }

  def changeType(df: DataFrame, columnType: String): DataFrame = {
    df.withColumn(column, col(column).cast(columnType))
  }

  def renameColumn(df: DataFrame, columnName: String): DataFrame = {
    df.withColumnRenamed(column, columnName)
  }

  def useConditions(df: DataFrame, conditionOptions: Map[String, String]): DataFrame = {
    val conditions = conditionOptions("conditions").split(",")
    var expression = "CASE"
    for (i <- conditions.indices) {
      val condition = conditions(i).split(":")
      expression += s" WHEN ${condition(0)} THEN ${condition(1)}"
    }
    if (conditionOptions.contains("otherwise"))
      expression += s" ELSE ${conditionOptions("otherwise")}"
    expression += " END"
    df.withColumn(column, expr(expression))
  }

  def useWindowFunction(df: DataFrame, winFuncOptions: Map[String, String]): DataFrame = {
    val windowFunction = winFuncOptions("windowFunction")
    var expression = ""
    if (Seq("count", "sum", "avg", "max", "min").contains(windowFunction)) {
      expression = s"$windowFunction(${winFuncOptions("column")})"
      expression += " OVER ("
      if (winFuncOptions.contains("partitionBy"))
        expression += s" PARTITION BY ${winFuncOptions("partitionBy").split(",").mkString(", ")}"
      if (winFuncOptions.contains("orderBy"))
        expression += s" ORDER BY ${winFuncOptions("orderBy").split(",")
                                                             .map(e => e.split(":").mkString(" "))
                                                             .mkString(", ")}"
      expression += ")"
    } else {
      expression = windowFunction match {
        case "rank" | "dense_rank" | "percent_rank" | "cume_dist" | "row_number" => s"$windowFunction()"
        case "ntile" => s"$windowFunction(${winFuncOptions("n")})"
        case "lag" | "lead" =>
          s"$windowFunction(" +
          s"${winFuncOptions("expression")}, " +
          s"${winFuncOptions.getOrElse("offset", 1)}, " +
          s"${winFuncOptions.getOrElse("defaultValue", null)})"
      }
      expression += " OVER ("
      if (winFuncOptions.contains("partitionBy"))
        expression += s" PARTITION BY ${winFuncOptions("partitionBy").split(",").mkString(", ")}"
      expression += s" ORDER BY ${winFuncOptions("orderBy").split(",")
                                                           .map(e => e.split(":").mkString(" "))
                                                           .mkString(", ")}"
      expression += " )"
    }
    df.withColumn(column, expr(expression))
  }

  def replaceValues(df: DataFrame, valueOptions: Map[String, String]): DataFrame = {
    val expression = s"regexp_replace($column, ${valueOptions("oldValue")}, ${valueOptions("newValue")})"
      .replace("\\", "\\"*3)
    df.withColumn(column, expr(expression))
  }

  def replaceValuesUsingConditions(df: DataFrame, conditionOptions: Map[String, String]): DataFrame = {
    val conditions = conditionOptions("conditions").split(",")
    var expression = "CASE"
    for (i <- conditions.indices) {
      val condition = conditions(i).split(":")
      expression += s" WHEN ${condition(0)} THEN regexp_replace($column, ${condition(1).split(";")
        .mkString(", ")})"
        .replace("\\", "\\"*3)
    }
    if (conditionOptions.contains("otherwise"))
      expression += s" ELSE ${conditionOptions("otherwise")}"
    expression += " END"
    df.withColumn(column, expr(expression))
  }

  def replaceValuesCharByChar(df: DataFrame, charOptions: Map[String, String]): DataFrame = {
    val expression = s"translate($column, ${charOptions("oldChars")}, ${charOptions("newChars")})"
    df.withColumn(column, expr(expression))
  }

  def useWithColumn(df: DataFrame): DataFrame = {
    operationType match {
      case "deriveColumn" => deriveColumn(df, options("expression"))
      case "addConstant" => addConstant(df, options("constant"))
      case "changeType" => changeType(df, options("columnType"))
      case "renameColumn" => renameColumn(df, options("columnName"))
      case "useConditions" => useConditions(df, options)
      case "useWindowFunction" => useWindowFunction(df, options)
      case "replaceValues" => replaceValues(df, options)
      case "replaceValuesUsingConditions" => replaceValuesUsingConditions(df, options)
      case "replaceValuesCharByChar" => replaceValuesCharByChar(df, options)
      case _ =>
       throw new TransformationConfigurationException(s"Operation $operationType does not exist")
    }
  }
}

object WithColumnStageBuilder extends StageBuilder {
  private val fieldColumn = "column"
  private val fieldOperationType = "operationType"

  override protected def validate(config: Map[String, String]): Boolean =
    config.get(fieldOperation).contains(OperationType.WITH_COLUMN.toString) &&
      config.contains(fieldColumn) &&
      config.contains(fieldOperationType)

  override protected def convert(config: Node): Stage = {
    val column = config.value(fieldColumn)
    val operationType = config.value(fieldOperationType)

    new WithColumnStage(config.id, column, operationType, getOptions(config.value))
  }
}
