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
import org.apache.spark.sql.functions.{ascii, base64, col, decode, encode, expr, format_number, initcap, instr, length, locate, lower, lpad, ltrim, regexp_extract, repeat, rpad, rtrim, split, substring, substring_index, trim, unbase64, upper}
import org.apache.spark.sql.{DataFrame, SparkSession}

private[function] final class StringStage(val id: String, function: String, options: Map[String, String]) extends Stage {
  override val operation: OperationType.Value = OperationType.STRING
  override val inputsRequired: Int = 1
  override val builder: StageBuilder = WithColumnStageBuilder

  override protected def process(input: Map[String, DataFrame])(implicit spark: SparkSession): Option[DataFrame] =
    input.values.headOption.map(useString)

  def asciiColumn(df: DataFrame, sourceColumn: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, ascii(col(sourceColumn)))
  }

  def base64Column(df: DataFrame, sourceColumn: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, base64(col(sourceColumn)))
  }

  def concatWs(df: DataFrame, sourceColumn: String, targetColumn: String, separator: String): DataFrame = {
    val expression = s"concat_ws(${s"'$separator'"}, $sourceColumn)"
    df.withColumn(targetColumn, expr(expression))
  }

  def decodeColumn(df: DataFrame, sourceColumn: String, targetColumn: String, charset: String): DataFrame = {
    df.withColumn(targetColumn, decode(col(sourceColumn), charset))
  }

  def encodeColumn(df: DataFrame, sourceColumn: String, targetColumn: String, charset: String): DataFrame = {
    df.withColumn(targetColumn, encode(col(sourceColumn), charset))
  }

  def formatNumber(df: DataFrame, sourceColumn: String, targetColumn: String, decimalPlaces: Int): DataFrame = {
    df.withColumn(targetColumn, format_number(col(sourceColumn), decimalPlaces))
  }

  def formatString(df: DataFrame, sourceColumn: String, targetColumn: String, formatString: String): DataFrame = {
    val expression = s"format_string(${s"'$formatString'"}, $sourceColumn)"
    df.withColumn(targetColumn, expr(expression))
  }

  def initcapColumn(df: DataFrame, sourceColumn: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, initcap(col(sourceColumn)))
  }

  def instrColumn(df: DataFrame, sourceColumn: String, targetColumn: String, substring: String): DataFrame = {
    df.withColumn(targetColumn, instr(col(sourceColumn), substring))
  }

  def lengthColumn(df: DataFrame, sourceColumn: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, length(col(sourceColumn)))
  }

  def lowerColumn(df: DataFrame, sourceColumn: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, lower(col(sourceColumn)))
  }

  def locateColumn(df: DataFrame, sourceColumn: String, targetColumn: String, substring: String, position: Int): DataFrame = {
    df.withColumn(targetColumn, locate(substring, col(sourceColumn), position))
  }

  def lpadColumn(df: DataFrame, sourceColumn: String, targetColumn: String, length: Int, pad: String): DataFrame = {
    df.withColumn(targetColumn, lpad(col(sourceColumn), length, pad))
  }

  def ltrimColumn(df: DataFrame, sourceColumn: String, targetColumn: String, trimString: String): DataFrame = {
    df.withColumn(targetColumn, ltrim(col(sourceColumn), trimString))
  }

  def regexpExtract(df: DataFrame, sourceColumn: String, targetColumn: String, regex: String, groupIndex: Int): DataFrame = {
    df.withColumn(targetColumn, regexp_extract(col(sourceColumn), regex.replace("\\", "\\"*3), groupIndex))
  }

  def unbase64Column(df: DataFrame, sourceColumn: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, unbase64(col(sourceColumn)))
  }

  def rpadColumn(df: DataFrame, sourceColumn: String, targetColumn: String, length: Int, pad: String): DataFrame = {
    df.withColumn(targetColumn, rpad(col(sourceColumn), length, pad))
  }

  def repeatColumn(df: DataFrame, sourceColumn: String, targetColumn: String, repeatNumber: Int): DataFrame = {
    df.withColumn(targetColumn, repeat(col(sourceColumn), repeatNumber))
  }

  def rtrimColumn(df: DataFrame, sourceColumn: String, targetColumn: String, trimString: String): DataFrame = {
    df.withColumn(targetColumn, rtrim(col(sourceColumn), trimString))
  }

  def splitColumn(df: DataFrame, sourceColumn: String, targetColumn: String, regex: String, limit: Int): DataFrame = {
    df.withColumn(targetColumn, split(col(sourceColumn), regex.replace("\\", "\\"*3), limit))
  }

  def substringColumn(df: DataFrame, sourceColumn: String, targetColumn: String, position: Int, length: Int): DataFrame = {
    df.withColumn(targetColumn, substring(col(sourceColumn), position, length))
  }

  def substringIndexColumn(df: DataFrame, sourceColumn: String, targetColumn: String, delimiter: String, count: Int): DataFrame = {
    df.withColumn(targetColumn, substring_index(col(sourceColumn), delimiter, count))
  }

  def trimColumn(df: DataFrame, sourceColumn: String, targetColumn: String, trimString: String): DataFrame = {
    df.withColumn(targetColumn, trim(col(sourceColumn), trimString))
  }

  def upperColumn(df: DataFrame, sourceColumn: String, targetColumn: String): DataFrame = {
    df.withColumn(targetColumn, upper(col(sourceColumn)))
  }

  def useString(df: DataFrame): DataFrame = {
    function match {
      case "ascii" => asciiColumn(df, options("sourceColumn"), options("targetColumn"))
      case "base64" => base64Column(df, options("sourceColumn"), options("targetColumn"))
      case "concat_ws" => concatWs(df, options("sourceColumn"), options("targetColumn"), options("separator"))
      case "decode" => decodeColumn(df, options("sourceColumn"), options("targetColumn"), options("charset"))
      case "encode" => encodeColumn(df, options("sourceColumn"), options("targetColumn"), options("charset"))
      case "format_number" => formatNumber(df, options("sourceColumn"), options("targetColumn"), options("decimalPlaces").toInt)
      case "format_string" => formatString(df, options("sourceColumn"), options("targetColumn"), options("formatString"))
      case "title_case" => initcapColumn(df, options("sourceColumn"), options("targetColumn"))
      case "instr" => instrColumn(df, options("sourceColumn"), options("targetColumn"), options("substring"))
      case "length" => lengthColumn(df, options("sourceColumn"), options("targetColumn"))
      case "lower" => lowerColumn(df, options("sourceColumn"), options("targetColumn"))
      case "locate" => locateColumn(df, options("sourceColumn"), options("targetColumn"), options("substring"), options.getOrElse("position", "1").toInt)
      case "lpad" => lpadColumn(df, options("sourceColumn"), options("targetColumn"), options("length").toInt,  options("pad"))
      case "ltrim" => ltrimColumn(df, options("sourceColumn"), options("targetColumn"), options.getOrElse("trimString", " "))
      case "regexp_extract" => regexpExtract(df, options("sourceColumn"), options("targetColumn"), options("regex"), options("groupIndex").toInt)
      case "unbase64" => unbase64Column(df, options("sourceColumn"), options("targetColumn"))
      case "rpad" => rpadColumn(df, options("sourceColumn"), options("targetColumn"), options("length").toInt,  options("pad"))
      case "repeat" => repeatColumn(df, options("sourceColumn"), options("targetColumn"), options("repeatNumber").toInt)
      case "rtrim" => rtrimColumn(df, options("sourceColumn"), options("targetColumn"), options.getOrElse("trimString", " "))
      case "split" => splitColumn(df, options("sourceColumn"), options("targetColumn"), options("regex"), options.getOrElse("limit", "-1").toInt)
      case "substring" => substringColumn(df, options("sourceColumn"), options("targetColumn"), options("position").toInt, options("length").toInt)
      case "substring_index" => substringIndexColumn(df, options("sourceColumn"), options("targetColumn"), options("delimiter"), options("count").toInt)
      case "trim" => trimColumn(df, options("sourceColumn"), options("targetColumn"), options.getOrElse("trimString", " "))
      case "upper" => upperColumn(df, options("sourceColumn"), options("targetColumn"))
      case _ =>
        throw new TransformationConfigurationException(s"Operation $function does not exist")
    }
  }
}

object StringStageBuilder extends StageBuilder {
  private val fieldFunction = "function"

  override protected def validate(config: Map[String, String]): Boolean =
    config.get(fieldOperation).contains(OperationType.STRING.toString) &&
      config.contains(fieldFunction)

  override protected def convert(config: Node): Stage = {
    new StringStage(config.id, config.value(fieldFunction), getOptions(config.value))
  }
}

