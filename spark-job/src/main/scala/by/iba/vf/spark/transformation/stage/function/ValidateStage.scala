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
import net.liftweb.json
import net.liftweb.json.JValue
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession


private[function] final class ValidateStage(val id: String, validateConfig: List[Map[String,String]], isError: Boolean) extends Stage {
  override val operation: OperationType.Value = OperationType.VALIDATE
  override val inputsRequired: Int = 1
  override val builder: StageBuilder = ValidateStageBuilder
  private implicit val Formats: json.DefaultFormats.type = json.DefaultFormats
  var fails: List[String] = Nil
  var success: List[String] = Nil

  override protected def process(input: Map[String, DataFrame])(implicit spark: SparkSession): Option[DataFrame] =
    input.values.headOption.map(useValidate)

  def dataType(df: DataFrame, column: String, dataType: String): Unit = {
    if (df.select(column).schema.toIndexedSeq(0).dataType.typeName != dataType)
      badFeedback(s"Column [$column] datatype is not $dataType")
  }

  def minValue(df: DataFrame, column: String, minValue: String): Unit = {
    if (df.filter(s"$column < $minValue").count() != 0)
      badFeedback(s"Column [$column] has value smaller than $minValue")
  }

  def maxValue(df: DataFrame, column: String, maxValue: String): Unit = {
    if (df.filter(s"$column > $maxValue").count() != 0)
      badFeedback(s"Column [$column] has value bigger than $maxValue")
  }

  def minLength(df: DataFrame, column: String, minLength: String): Unit = {
    if (df.filter(s"LENGTH($column) < $minLength").count() != 0)
      badFeedback(s"Column [$column] has value shorter than $minLength symbols")
  }

  def maxLength(df: DataFrame, column: String, maxLength: String): Unit = {
    if (df.filter(s"LENGTH($column) > $maxLength").count() != 0)
      badFeedback(s"Column [$column] has value longer than $maxLength symbols")
  }

  def notNull(df: DataFrame, column: String): Unit = {
    if (df.filter(s"$column IS NULL").count() != 0)
      badFeedback(s"Column [$column] contains NULL value")
  }

  def inValues(df: DataFrame, column: String, values: String): Unit = {
    if (df.filter(!col(column).isin(values.split(','):_*)).count() != 0)
      badFeedback(s"Column [$column] contains value not in ($values)")
  }

  def regex(df: DataFrame, column: String, regex: String): Unit = {
    if (df.filter(col(column).rlike(regex)).count() != df.count())
      badFeedback(s"Column [$column] contains value that doesn't match '$regex'")
  }

  def uniqueness(df: DataFrame, column: String): Unit = {
    if (df.dropDuplicates(column.split(",")).count() != df.count())
      badFeedback(s"Column(s) [$column] contain duplicates")
  }

  def badFeedback(message: String): Unit = {
    fails = message :: fails
  }

  def parseValidations(validations: String): List[Map[String,String]] = {
    json.parse(validations).extract[List[Map[String,String]]]
  }

  def useValidate(df: DataFrame): DataFrame = {
    validateConfig.foreach {
      columnConfig =>
        val column = columnConfig("column")
        val validations = parseValidations(columnConfig("validations"))
      if (column.contains(',')) {
        validations.foreach {
          validation =>
            validation("type") match {
              case "uniqueness" => uniqueness(df, column)
              case _ =>
                throw new TransformationConfigurationException(s"Validation type ${validation("type")} for $column does not exist")
            }
        }
      }
      else {
        validations.foreach {
          validation =>
            validation("type") match {
              case "dataType" => dataType(df, column, validation("dataType"))
              case "minValue" => minValue(df, column, validation("minValue"))
              case "maxValue" => maxValue(df, column, validation("maxValue"))
              case "minLength" => minLength(df, column, validation("minLength"))
              case "maxLength" => maxLength(df, column, validation("maxLength"))
              case "notNull" => notNull(df, column)
              case "inValues" => inValues(df, column, validation("inValues"))
              case "regex" => regex(df, column, validation("regex"))
              case "uniqueness" => uniqueness(df, column)
              case _ =>
                throw new TransformationConfigurationException(s"Validation type ${validation("type")} for $column does not exist")
            }
        }
      }
        if (fails.count(s => s.contains(s"[$column]")) == 0)
          success = (s"All ${validations.length} checks for [$column] column(s) passed successfully") :: success
    }

    if (success.nonEmpty) {
      logger.info(success.mkString("\n"))
    }
    if (fails.nonEmpty){
      if (!isError) {
        logger.warn(fails.mkString("\n"))
      }
      else {
        logger.error(fails.mkString("\n"))
        throw new TransformationConfigurationException(fails.mkString("\n"))
      }
    }
    df
  }
}

object ValidateStageBuilder extends StageBuilder {
  private implicit val Formats: json.DefaultFormats.type = json.DefaultFormats
  private val fieldValidateConfig = "validateConfig"
  private val fieldIsError = "isError"

  def parseValidateConfig(validateConfig: String): List[Map[String,String]] = {
    json.parse(validateConfig).extract[List[Map[String,String]]]
  }

  override protected def validate(config: Map[String, String]): Boolean =
    config.get("operation").contains(OperationType.VALIDATE.toString) &&
      config.contains(fieldValidateConfig)

  override protected def convert(config: Node): Stage = {
    val validateConfig = config.value(fieldValidateConfig)
    val isError = config.value.getOrElse(fieldIsError, "true").toBoolean

    new ValidateStage(config.id, parseValidateConfig(validateConfig), isError)
  }
}

