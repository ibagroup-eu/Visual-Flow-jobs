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
package by.iba.vf.spark.transformation.stage.config.formats

import by.iba.vf.spark.transformation.config.Node

object CSV extends TextBasedStructured {
  private val fieldSamplingRatio = "samplingRatio"
  private val fieldQuote = "quote"
  private val fieldQuoteAll = "quoteAll"
  private val fieldEscape = "escape"
  private val fieldEscapeQuotes = "escapeQuotes"
  private val fieldInferSchema = "inferSchema"
  private val fieldEnforceSchema = "enforceSchema"
  private val fieldIgnoreLeadingWhiteSpace= "ignoreLeadingWhiteSpace"
  private val fieldIgnoreTrailingWhiteSpace = "ignoreTrailingWhiteSpace"
  private val fieldNullValue = "nullValue"
  private val fieldNanValue = "nanValue"
  private val fieldDelimiter = "delimiter"
  private val fieldHeader = "header"

  override def buildConnectionOptions(config: Node): Map[String, String] = {
    val samplingRatio = config.value.get(fieldSamplingRatio)
    val quote = config.value.get(fieldQuote)
    val quoteAll = config.value.get(fieldQuoteAll)
    val escape = config.value.get(fieldEscape)
    val escapeQuotes = config.value.get(fieldEscapeQuotes)
    val inferSchema = config.value.get(fieldInferSchema)
    val enforceSchema = config.value.get(fieldEnforceSchema)
    val ignoreLeadingWhiteSpace = config.value.get(fieldIgnoreLeadingWhiteSpace)
    val ignoreTrailingWhiteSpace = config.value.get(fieldIgnoreTrailingWhiteSpace)
    val nullValue = config.value.get(fieldNullValue)
    val nanValue = config.value.get(fieldNanValue)
    val delimiter = config.value.get(fieldDelimiter)
    val header = config.value.get(fieldHeader)

    var configMap = super.buildConnectionOptions(config)
    samplingRatio.foreach { configMap += "samplingRatio" -> _ }
    quote.foreach { configMap += "quote" -> _ }
    quoteAll.foreach { configMap += "quoteAll" -> _ }
    escape.foreach { configMap += "escape" -> _ }
    escapeQuotes.foreach { configMap += "escapeQuotes" -> _ }
    inferSchema.foreach { configMap += "inferSchema" -> _ }
    enforceSchema.foreach { configMap += "enforceSchema" -> _ }
    ignoreLeadingWhiteSpace.foreach { configMap += "ignoreLeadingWhiteSpace" -> _ }
    ignoreTrailingWhiteSpace.foreach { configMap += "ignoreTrailingWhiteSpace" -> _ }
    nullValue.foreach { configMap += "nullValue" -> _ }
    nanValue.foreach { configMap += "nanValue" -> _ }
    delimiter.foreach { configMap += "delimiter" -> _ }
    header.foreach { configMap += "header" -> _ }

    configMap
  }

}
