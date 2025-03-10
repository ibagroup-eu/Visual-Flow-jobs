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
package by.iba.vf.spark.transformation.utils.ai

import by.iba.vf.spark.transformation.exception.TransformationConfigurationException
import by.iba.vf.spark.transformation.stage.function.{AITextTaskStageConfig, AttributeConfiguration}
import by.iba.vf.spark.transformation.utils.ai.llm.OpenAIChatGPT
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, udf, from_json}
import org.apache.spark.sql.types.{StructType, StructField, StringType}

object GenAIUtils {

  def parseText(spark: SparkSession,
                df: DataFrame,
                config: AITextTaskStageConfig): DataFrame = {
    val parseTextUdf = udf(delegateParseTextToLLM _)
    spark.udf.register("parseContent", parseTextUdf)
    val systemMessage = buildParseTextSystemMessage(config)
    val fieldList = config.attributes.attributes.map(a => s"${a.name}").mkString(", ")
    val schema = StructType(config.attributes.attributes.map(a => s"${a.name}").map(StructField(_, StringType)))
    df
      .withColumn("parsed", parseTextUdf(
        col(config.sourceColumn),
        lit(config.llm),
        lit(config.endpoint),
        lit(config.model),
        lit(config.apiKey),
        lit(config.temperature),
        lit(config.maxTokens),
        lit(config.keepExtraAttributes),
        lit(fieldList),
        lit(systemMessage)))
      .withColumn("parsed", from_json(col("parsed"), schema))
      .select("*", "parsed.*")
      .drop("parsed")
  }

  private def buildParseTextSystemMessage(config: AITextTaskStageConfig): String = {
    val terminology = config.attributes.attributes.map(a =>
      s"${a.name.toLowerCase.replace('_', ' ').capitalize} - " +
        s"${Option(a.definition).filterNot(_.isEmpty).getOrElse(a.name.toLowerCase.replace('_', ' ').capitalize)}, " +
        s"if ${a.name.toLowerCase.replace('_', ' ').capitalize} is unknown put value '${a.placeholder.getOrElse("")}'").mkString("\n")
    val examplesLine = config.examples.examples.map(e => s"User: ${e.user}\nAssistant: ${e.assistant.trim}").mkString("\n---\n")
    s"""
      |${config.systemMessage}. You provide output in JSON format. Values of all attributes must be provided as strings.
      |Terminology:
      |$terminology
      |Examples:
      |$examplesLine
    """.stripMargin
  }

  private def delegateParseTextToLLM(prompt: String,
                                     llm: String,
                                     endpoint: String,
                                     model: String,
                                     apiKey: String,
                                     temperature: String,
                                     maxTokens: String,
                                     keepExtraAttributes: String,
                                     fieldList: String,
                                     systemMessage: String): String = {
    val userMessage = s"Extract fields $fieldList from text: $prompt."
    llm match {
      case "ChatGPT" => OpenAIChatGPT.processRequest(endpoint, model, apiKey, temperature, systemMessage, userMessage)
      case _ =>
        throw new TransformationConfigurationException(s"LLM $llm does not exist")
    }
  }

  def generateData(spark: SparkSession,
                   config: AITextTaskStageConfig): DataFrame = {
    val generateDataUdf = udf(delegateGenerateDataToLLM _)
    spark.udf.register("generateData", generateDataUdf)
    val systemMessage = buildGenerateDataSystemMessage(config)
    val fieldList = config.attributes.attributes.map(a => s"${a.name}").mkString(", ")
    val schema = StructType(config.attributes.attributes.map(a => s"${a.name}").map(StructField(_, StringType)))
    val df = spark.range(config.numberOfRecords.toLong)
    df
      .withColumn("generated", generateDataUdf(
        lit(config.llm),
        lit(config.endpoint),
        lit(config.model),
        lit(config.apiKey),
        lit(config.temperature),
        lit(config.maxTokens),
        lit(fieldList),
        lit(systemMessage)))
      .withColumn("generated", from_json(col("generated"), schema))
      .select("generated.*")
  }

  private def buildGenerateDataSystemMessage(config: AITextTaskStageConfig): String = {
    val terminology = config.attributes.attributes.map(a =>
      s"${a.name.toLowerCase.replace('_', ' ').capitalize} - " +
        s"${Option(a.definition).filterNot(_.isEmpty).getOrElse(a.name.toLowerCase.replace('_', ' ').capitalize)}").mkString("\n")
    val examplesLine = config.examples.examples.map(e => s"User: ${e.user}\nAssistant: ${e.assistant.trim}").mkString("\n---\n")
    s"""
       |${config.systemMessage}. You provide output in JSON format. Values of all attributes must be provided as strings.
       |Terminology:
       |$terminology
       |Examples:
       |$examplesLine
    """.stripMargin
  }

  private def delegateGenerateDataToLLM(llm: String,
                                        endpoint: String,
                                        model: String,
                                        apiKey: String,
                                        temperature: String,
                                        maxTokens: String,
                                        fieldList: String,
                                        systemMessage: String): String = {
    val userMessage = s"Generate synthetic data of one record with the following fields: $fieldList"
    llm match {
      case "ChatGPT" => OpenAIChatGPT.processRequest(endpoint, model, apiKey, temperature, systemMessage, userMessage)
      case _ =>
        throw new TransformationConfigurationException(s"LLM $llm does not exist")
    }
  }

  def genericTask(spark: SparkSession,
                  df: DataFrame,
                  config: AITextTaskStageConfig): DataFrame = {
    val genericTaskUdf = udf(delegateGenericTaskToLLM _)
    spark.udf.register("genericTask", genericTaskUdf)
    df
      .withColumn(config.outputColumn, genericTaskUdf(
        lit(config.llm),
        lit(config.endpoint),
        lit(config.model),
        lit(config.apiKey),
        lit(config.temperature),
        lit(config.maxTokens),
        lit(config.systemMessage),
        lit(config.userMessage),
        col(config.sourceColumn)))
  }

  private def delegateGenericTaskToLLM(llm: String,
                                       endpoint: String,
                                       model: String,
                                       apiKey: String,
                                       temperature: String,
                                       maxTokens: String,
                                       systemMessage: String,
                                       prompt: String,
                                       text: String): String = {
    val userMessage = s"$prompt. Text: $text"
    llm match {
      case "ChatGPT" => OpenAIChatGPT.processRequest(endpoint, model, apiKey, temperature, systemMessage, userMessage)
      case _ =>
        throw new TransformationConfigurationException(s"LLM $llm does not exist")
    }
  }

  def transcribe(spark: SparkSession,
                 df: DataFrame,
                 config: AITextTaskStageConfig): DataFrame = {
    val transcribeUdf = udf(delegateTranscribeToLLM _)
    spark.udf.register("transcribe", transcribeUdf)
    df
      .withColumn(config.outputColumn, transcribeUdf(
        lit(config.llm),
        lit(config.endpoint),
        lit(config.model),
        lit(config.apiKey),
        lit(config.temperature),
        col(config.pathColumn),
        col(config.sourceColumn)))
      .drop(config.sourceColumn)
  }

  private def delegateTranscribeToLLM(llm: String,
                                      endpoint: String,
                                      model: String,
                                      apiKey: String,
                                      temperature: String,
                                      fileName: String,
                                      content: Array[Byte]): String = {
    llm match {
      case "ChatGPT" => OpenAIChatGPT.processMultiPartRequest(endpoint, model, apiKey, temperature, fileName, content)
      case _ =>
        throw new TransformationConfigurationException(s"LLM $llm does not exist")
    }
  }

}
