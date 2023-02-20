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
import org.apache.spark.sql.functions.{add_months, col, current_date, current_timestamp, date_add, date_format, date_sub, date_trunc, datediff, dayofmonth, dayofweek, dayofyear, from_unixtime, hour, last_day, minute, month, months_between, next_day, quarter, second, to_date, to_timestamp, trunc, unix_timestamp, weekofyear, year}
import org.apache.spark.sql.{DataFrame, SparkSession}


private[function] final class DateTimeStage(val id: String, function: String, options: Map[String, String]) extends Stage {
  override val operation: OperationType.Value = OperationType.DATETIME
  override val inputsRequired: Int = 1
  override val builder: StageBuilder = DateTimeStageBuilder

  override protected def process(input: Map[String, DataFrame])(implicit spark: SparkSession): Option[DataFrame] =
    input.values.headOption.map(useDateTime)

  def useDateTime(df: DataFrame): DataFrame = {
    function match {
      // Date Function
      case "current_date" => currentDate(df, options("targetColumn"))
      case "date_format" => dateFormat(df, options("sourceColumn"), options("format"), options("targetColumn"))
      case "to_date" => toDate(df, options("sourceColumn"),  options("format"), options("targetColumn"))
      case "add_months" => addMonths(df, options("sourceColumn"),  options("numMonths").toInt, options("targetColumn"))
      case "date_add" => dateAdd(df, options("sourceColumn"),  options("days").toInt, options("targetColumn"))
      case "date_sub" => dateSub(df, options("sourceColumn"),  options("days").toInt, options("targetColumn"))
      case "datediff" => dateDiff(df, options("endColumn"),  options("startColumn"), options("targetColumn"))
      case "months_between" => monthsBetween(df, options("endColumn"),  options("startColumn"), options("roundOff").toBoolean, options("targetColumn"))
      case "next_day" => nextDay(df, options("sourceColumn"),  options("dayOfWeek"), options("targetColumn"))
      case "year" => yearColumn(df, options("sourceColumn"), options("targetColumn"))
      case "quarter" => quarterColumn(df, options("sourceColumn"), options("targetColumn"))
      case "month" => monthColumn(df, options("sourceColumn"), options("targetColumn"))
      case "dayofweek" => dayofweekColumn(df, options("sourceColumn"), options("targetColumn"))
      case "dayofmonth" => dayofmonthColumn(df, options("sourceColumn"), options("targetColumn"))
      case "dayofyear" => dayofyearColumn(df, options("sourceColumn"), options("targetColumn"))
      case "weekofyear" => weekofyearColumn(df, options("sourceColumn"), options("targetColumn"))
      case "last_day" => lastDay(df, options("sourceColumn"), options("targetColumn"))
      case "trunc" => truncColumn(df, options("sourceColumn"), options("format"), options("targetColumn"))
      // Timestamp Function
      case "current_timestamp" => currentTimestamp(df, options("targetColumn"))
      case "hour" => hourColumn(df, options("sourceColumn"), options("targetColumn"))
      case "minute" => minuteColumn(df, options("sourceColumn"), options("targetColumn"))
      case "second" => secondColumn(df, options("sourceColumn"), options("targetColumn"))
      case "to_timestamp" => toTimestamp(df, options("sourceColumn"), options("format"), options("targetColumn"))
      case "date_trunc" => dateTrunc(df, options("sourceColumn"), options("format"), options("targetColumn"))
      case "unix_timestamp" => unixTimestamp(df, options("targetColumn"))
      case "to_unix_timestamp" => toUnixTimestamp(df, options("sourceColumn"), options("format"), options("targetColumn"))
      case "from_unixtime" => fromUnixtime(df, options("sourceColumn"), options("format"), options("targetColumn"))
      case _ =>
        throw new TransformationConfigurationException(s"Operation $function does not exist")
    }
  }

  def currentDate(df: DataFrame, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, current_date())
  }

  def dateFormat(df: DataFrame, sourceColumn: String, format: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, date_format(col(sourceColumn), format))
  }

  def toDate(df: DataFrame, sourceColumn: String, format: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, to_date(col(sourceColumn), format))
  }

  def addMonths(df: DataFrame, sourceColumn: String, numMonths: Int, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, add_months(col(sourceColumn), numMonths))
  }

  def dateAdd(df: DataFrame, sourceColumn: String, days: Int, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, date_add(col(sourceColumn), days))
  }

  def dateSub(df: DataFrame, sourceColumn: String, days: Int, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, date_sub(col(sourceColumn), days))
  }

  def dateDiff(df: DataFrame, endColumn: String, startColumn: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, datediff(col(endColumn), col(startColumn)))
  }

  def monthsBetween(df: DataFrame, endColumn: String, startColumn: String, roundOff: Boolean, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, months_between(col(endColumn), col(startColumn), roundOff))
  }

  def nextDay(df: DataFrame, sourceColumn: String, dayOfWeek: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, next_day(col(sourceColumn), dayOfWeek))
  }

  def yearColumn(df: DataFrame, sourceColumn: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, year(col(sourceColumn)))
  }

  def quarterColumn(df: DataFrame, sourceColumn: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, quarter(col(sourceColumn)))
  }

  def monthColumn(df: DataFrame, sourceColumn: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, month(col(sourceColumn)))
  }

  def dayofweekColumn(df: DataFrame, sourceColumn: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, dayofweek(col(sourceColumn)))
  }

  def dayofmonthColumn(df: DataFrame, sourceColumn: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, dayofmonth(col(sourceColumn)))
  }

  def dayofyearColumn(df: DataFrame, sourceColumn: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, dayofyear(col(sourceColumn)))
  }

  def weekofyearColumn(df: DataFrame, sourceColumn: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, weekofyear(col(sourceColumn)))
  }

  def lastDay(df: DataFrame, sourceColumn: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, last_day(col(sourceColumn)))
  }

  def truncColumn(df: DataFrame, sourceColumn: String, format: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, trunc(col(sourceColumn), format))
  }

  def currentTimestamp(df: DataFrame, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, current_timestamp())
  }

  def hourColumn(df: DataFrame, sourceColumn: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, hour(col(sourceColumn)))
  }

  def minuteColumn(df: DataFrame, sourceColumn: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, minute(col(sourceColumn)))
  }

  def secondColumn(df: DataFrame, sourceColumn: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, second(col(sourceColumn)))
  }

  def toTimestamp(df: DataFrame, sourceColumn: String, format: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, to_timestamp(col(sourceColumn), format))
  }

  def dateTrunc(df: DataFrame, sourceColumn: String, format: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, date_trunc(format, col(sourceColumn)))
  }

  def unixTimestamp(df: DataFrame, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, unix_timestamp())
  }

  def toUnixTimestamp(df: DataFrame, sourceColumn: String, format: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, unix_timestamp(col(sourceColumn), format))
  }

  def fromUnixtime(df: DataFrame, sourceColumn: String, format: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, from_unixtime(col(sourceColumn), format))
  }

}

object DateTimeStageBuilder extends StageBuilder {
  private val fieldFunction = "function"

  override protected def validate(config: Map[String, String]): Boolean =
    config.get(fieldOperation).contains(OperationType.DATETIME.toString) && config.contains(fieldFunction)

  override protected def convert(config: Node): Stage =
    new DateTimeStage(config.id, config.value(fieldFunction), getOptions(config.value))
}