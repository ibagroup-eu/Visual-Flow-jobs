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
import by.iba.vf.spark.transformation.stage.function.AITextTaskStageConfig._
import by.iba.vf.spark.transformation.utils.ai.GenAIUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Base64
import net.liftweb.json
import net.liftweb.json.DefaultFormats

import java.nio.charset.StandardCharsets

private[function] final class AITextTaskStage(val configNode: Node, config: AITextTaskStageConfig) extends Stage {
  override val operation: OperationType.Value = OperationType.AI_TEXT_TASK
  override val inputsRequired: Int = 1
  override val builder: StageBuilder = AITextTaskStageBuilder

  override protected def process(input: Map[String, DataFrame])(implicit spark: SparkSession): Option[DataFrame] =
    Some(performTask(spark, input.values.headOption.get))

  private def performTask(spark: SparkSession, df: DataFrame): DataFrame = {
    config.task match {
      case "parseText" => GenAIUtils.parseText(spark, df, config)
      case "generateData" => GenAIUtils.generateData(spark, config)
      case "genericTask" => GenAIUtils.genericTask(spark, df, config)
      case "transcribe" => GenAIUtils.transcribe(spark, df, config)
      case _ =>
        throw new TransformationConfigurationException(s"Task ${config.task} does not exist")
    }
  }

}

final case class Attribute(name: String, definition: String, placeholder: Option[String], confidenceThreshold: Option[String])
final case class AttributeConfiguration(attributes: Seq[Attribute])
final case class Example(user: String, assistant: String)
final case class ExampleConfiguration(examples: Seq[Example])

object AITextTaskStageConfig {
  private implicit val Formats: json.DefaultFormats.type = json.DefaultFormats

  val fieldTask = "task"
  val fieldLLM = "llm"
  val fieldEndpoint = "endpoint"
  val fieldModel = "model"
  val fieldAPIKey = "apiKey"
  val fieldSourceColumn = "sourceColumn"
  val fieldPathColumn = "pathColumn"
  val fieldOutputColumn = "outputColumn"
  val fieldNumberOfRecords = "numberOfRecords"
  private val fieldMaxTokens = "maxTokens"
  private val fieldTemperature = "temperature"
  private val fieldSystemMessage = "systemMessage"
  val fieldUserMessage = "userMessage"
  private val fieldKeepExtraAttributes = "keepExtraAttributes"
  val fieldAttributes = "attributes"
  private val fieldExamples = "examples"

  def parseAttributes(encodedJson: Option[String]): AttributeConfiguration = {
    val placeholder = new String(Base64.getEncoder.encode("""{"attributes": []}""".getBytes), StandardCharsets.UTF_8)
    val decodedJson = new String(Base64.getDecoder.decode(encodedJson.getOrElse(placeholder)))
    json.parse(decodedJson)
      .extract[AttributeConfiguration]
  }

  private def parseExamples(encodedJson: Option[String]): ExampleConfiguration = {
    val placeholder = new String(Base64.getEncoder.encode("""{"examples": []}""".getBytes), StandardCharsets.UTF_8)
    val decodedJson = new String(Base64.getDecoder.decode(encodedJson.getOrElse(placeholder)))
    val examples = json.parse(decodedJson)
      .extract[ExampleConfiguration]
    ExampleConfiguration(examples.examples.map(
      e => Example(e.user, new String(Base64.getDecoder.decode(e.assistant)))))
  }

}

class AITextTaskStageConfig(config: Node) {
  val task: String = config.value(fieldTask)
  val llm: String = config.value(fieldLLM)
  val endpoint: String = config.value(fieldEndpoint)
  val model: String = config.value(fieldModel)
  val apiKey: String = config.value(fieldAPIKey)
  val sourceColumn: String = config.value.getOrElse(fieldSourceColumn, "")
  val pathColumn: String = config.value.getOrElse(fieldPathColumn, "")
  val outputColumn: String = config.value.getOrElse(fieldOutputColumn, "")
  val numberOfRecords: String = config.value.getOrElse(fieldNumberOfRecords, "")
  val maxTokens: String = config.value.getOrElse(fieldMaxTokens, "")
  val temperature: String = config.value.getOrElse(fieldTemperature, "")
  val systemMessage: String = config.value.getOrElse(fieldSystemMessage, "")
  val userMessage: String = config.value.getOrElse(fieldUserMessage, "")
  val keepExtraAttributes: String = config.value.getOrElse(fieldKeepExtraAttributes, "false")
  val attributes: AttributeConfiguration = parseAttributes(config.value.get(fieldAttributes))
  val examples: ExampleConfiguration = parseExamples(config.value.get(fieldExamples))
}

object AITextTaskStageBuilder extends StageBuilder {

  override protected def validate(config: Map[String, String]): Boolean =
    config.get(fieldOperation).contains(OperationType.AI_TEXT_TASK.toString) &&
      config.contains(AITextTaskStageConfig.fieldTask) && config.contains(AITextTaskStageConfig.fieldTask) &&
      config.contains(AITextTaskStageConfig.fieldLLM) && config.contains(AITextTaskStageConfig.fieldEndpoint) &&
      config.contains(AITextTaskStageConfig.fieldModel) && config.contains(AITextTaskStageConfig.fieldAPIKey) &&
      ((config.contains(AITextTaskStageConfig.fieldSourceColumn) && List("parseText", "genericTask", "transcribe").contains(config(fieldTask))) ||
        (config.contains(AITextTaskStageConfig.fieldNumberOfRecords) && config(fieldTask) == "generateData")) &&
      (validateAttributes(config.get(AITextTaskStageConfig.fieldAttributes)) && List("parseText", "generateData").contains(config(fieldTask)) ||
        List("genericTask", "transcribe").contains(config(fieldTask))) &&
      (config.contains(AITextTaskStageConfig.fieldUserMessage) && config(fieldTask) == "genericTask" || config(fieldTask) != "genericTask") &&
      (config.contains(AITextTaskStageConfig.fieldOutputColumn) && List("genericTask", "transcribe").contains(config(fieldTask)) ||
        !List("genericTask", "transcribe").contains(config(fieldTask))) &&
      (config.contains(AITextTaskStageConfig.fieldPathColumn) && config(fieldTask) == "transcribe" || config(fieldTask) != "transcribe")

  private def validateAttributes(encodedJson: Option[String]): Boolean =
      AITextTaskStageConfig.parseAttributes(encodedJson).attributes.nonEmpty

  override protected def convert(config: Node): Stage =
    new AITextTaskStage(config, new AITextTaskStageConfig(config))
}
