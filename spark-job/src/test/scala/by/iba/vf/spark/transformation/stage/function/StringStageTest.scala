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
import by.iba.vf.spark.transformation.stage.{OperationType, Stage}
import org.apache.spark.sql.functions.{ascii, base64, col, decode, encode, expr, format_number, initcap, instr, length, locate, lower, lpad, ltrim, regexp_extract, repeat, rpad, rtrim, split, substring, substring_index, trim, unbase64, upper}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers._

class StringStageTest extends AnyFunSpec with MockitoSugar with PrivateMethodTester {
  val configs: List[Map[String, String]] = List(
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "ascii",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "base64",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "concat_ws",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.separator" -> "separator"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "decode",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.charset" -> "charset"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "encode",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.charset" -> "charset"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "format_number",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.decimalPlaces" -> "2"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "format_string",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.formatString" -> "formatString"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "title_case",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "instr",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.substring" -> "substring"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "length",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "lower",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "locate",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.substring" -> "substring",
      "option.position" -> "2"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "lpad",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.substring" -> "substring",
      "option.length" -> "5",
      "option.pad" -> "pad"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "ltrim",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.trimString" -> "trimString"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "regexp_extract",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.regex" -> "regex",
      "option.groupIndex" -> "2"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "unbase64",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "rpad",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.length" -> "5",
      "option.pad" -> "pad"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "repeat",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.repeatNumber" -> "3"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "rtrim",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.trimString" -> "trimString"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "split",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.regex" -> "regex",
      "option.limit" -> "2"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "substring",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.position" -> "2",
      "option.length" -> "2"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "substring_index",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.delimiter" -> "delimiter",
      "option.count" -> "5"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "trim",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.trimString" -> "trimString"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "upper",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b"
    )
  )

  private def getOptions(config: Map[String, String]): Map[String, String] = {
    val optionPattern = "option\\.(.+)".r
    config
      .flatMap {
        case (optionPattern(optionName), optionValue) => Some(optionName -> optionValue)
        case _ => None
      }
  }

  it("process") {
    implicit lazy val spark: SparkSession = mock[SparkSession]
    val df = mock[DataFrame]
    val df2 = mock[DataFrame]

    for (config <- configs) {
      if (config("function") == "ascii")
        when(df.withColumn(getOptions(config)("targetColumn"), ascii(col(getOptions(config)("sourceColumn")))))
          .thenReturn(df2)
      else if (config("function") == "base64")
        when(df.withColumn(getOptions(config)("targetColumn"), base64(col(getOptions(config)("sourceColumn")))))
          .thenReturn(df2)
      else if (config("function") == "concat_ws")
        when(df.withColumn(getOptions(config)("targetColumn"),
          expr(s"concat_ws(${s"'${getOptions(config)("separator")}'"}, ${getOptions(config)("sourceColumn")})")))
          .thenReturn(df2)
      else if (config("function") == "decode")
        when(df.withColumn(getOptions(config)("targetColumn"), decode(col(getOptions(config)("sourceColumn")),
          getOptions(config)("charset"))))
          .thenReturn(df2)
      else if (config("function") == "encode")
        when(df.withColumn(getOptions(config)("targetColumn"), encode(col(getOptions(config)("sourceColumn")),
          getOptions(config)("charset"))))
          .thenReturn(df2)
      else if (config("function") == "format_number")
        when(df.withColumn(getOptions(config)("targetColumn"), format_number(col(getOptions(config)("sourceColumn")),
          getOptions(config)("decimalPlaces").toInt)))
          .thenReturn(df2)
      else if (config("function") == "format_string")
        when(df.withColumn(getOptions(config)("targetColumn"),
          expr(s"format_string(${s"'${getOptions(config)("formatString")}'"}, ${getOptions(config)("sourceColumn")})")))
          .thenReturn(df2)
      else if (config("function") == "title_case")
        when(df.withColumn(getOptions(config)("targetColumn"), initcap(col(getOptions(config)("sourceColumn")))))
          .thenReturn(df2)
      else if (config("function") == "instr")
        when(df.withColumn(getOptions(config)("targetColumn"), instr(col(getOptions(config)("sourceColumn")),
          getOptions(config)("substring"))))
          .thenReturn(df2)
      else if (config("function") == "length")
        when(df.withColumn(getOptions(config)("targetColumn"), length(col(getOptions(config)("sourceColumn")))))
          .thenReturn(df2)
      else if (config("function") == "lower")
        when(df.withColumn(getOptions(config)("targetColumn"), lower(col(getOptions(config)("sourceColumn")))))
          .thenReturn(df2)
      else if (config("function") == "locate")
        when(df.withColumn(getOptions(config)("targetColumn"), locate(getOptions(config)("substring"), col(getOptions(config)("sourceColumn")),
          getOptions(config)("position").toInt)))
          .thenReturn(df2)
      else if (config("function") == "lpad")
        when(df.withColumn(getOptions(config)("targetColumn"), lpad(col(getOptions(config)("sourceColumn")),
          getOptions(config)("length").toInt, getOptions(config)("pad"))))
          .thenReturn(df2)
      else if (config("function") == "ltrim")
        when(df.withColumn(getOptions(config)("targetColumn"), ltrim(col(getOptions(config)("sourceColumn")),
          getOptions(config)("trimString"))))
          .thenReturn(df2)
      else if (config("function") == "regexp_extract")
        when(df.withColumn(getOptions(config)("targetColumn"), regexp_extract(col(getOptions(config)("sourceColumn")),
          getOptions(config)("regex").replace("\\", "\\" * 3), getOptions(config)("groupIndex").toInt)))
          .thenReturn(df2)
      else if (config("function") == "unbase64")
        when(df.withColumn(getOptions(config)("targetColumn"), unbase64(col(getOptions(config)("sourceColumn")))))
          .thenReturn(df2)
      else if (config("function") == "rpad")
        when(df.withColumn(getOptions(config)("targetColumn"), rpad(col(getOptions(config)("sourceColumn")),
          getOptions(config)("length").toInt, getOptions(config)("pad"))))
          .thenReturn(df2)
      else if (config("function") == "repeat")
        when(df.withColumn(getOptions(config)("targetColumn"), repeat(col(getOptions(config)("sourceColumn")),
          getOptions(config)("repeatNumber").toInt)))
          .thenReturn(df2)
      else if (config("function") == "rtrim")
        when(df.withColumn(getOptions(config)("targetColumn"), rtrim(col(getOptions(config)("sourceColumn")),
          getOptions(config)("trimString"))))
          .thenReturn(df2)
      else if (config("function") == "split")
        when(df.withColumn(getOptions(config)("targetColumn"), split(col(getOptions(config)("sourceColumn")),
          getOptions(config)("regex").replace("\\", "\\" * 3), getOptions(config)("limit").toInt)))
          .thenReturn(df2)
      else if (config("function") == "substring")
        when(df.withColumn(getOptions(config)("targetColumn"), substring(col(getOptions(config)("sourceColumn")),
          getOptions(config)("position").toInt, getOptions(config)("length").toInt)))
          .thenReturn(df2)
      else if (config("function") == "substring_index")
        when(df.withColumn(getOptions(config)("targetColumn"), substring_index(col(getOptions(config)("sourceColumn")),
          getOptions(config)("delimiter"), getOptions(config)("count").toInt)))
          .thenReturn(df2)
      else if (config("function") == "trim")
        when(df.withColumn(getOptions(config)("targetColumn"), trim(col(getOptions(config)("sourceColumn")),
          getOptions(config)("trimString"))))
          .thenReturn(df2)
      else if (config("function") == "upper")
        when(df.withColumn(getOptions(config)("targetColumn"), upper(col(getOptions(config)("sourceColumn")))))
          .thenReturn(df2)

      val stage = new StringStage(Node("id", Map()), config("function"), getOptions(config))
      val result = stage invokePrivate PrivateMethod[Option[DataFrame]]('process)(Map("1" -> df), spark)
      result should be(Some(df2))
    }
  }
}

class StringStageBuilderTest extends AnyFunSpec with PrivateMethodTester {
  val configs: List[Map[String, String]] = List(
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "ascii",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "base64",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "concat_ws",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.separator" -> "separator"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "decode",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.charset" -> "charset"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "encode",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.charset" -> "charset"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "format_number",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.decimalPlaces" -> "2"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "format_string",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.formatString" -> "formatString"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "title_case",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "instr",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.substring" -> "substring"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "length",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "lower",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "locate",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.substring" -> "substring"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "locate",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.substring" -> "substring",
      "option.position" -> "2"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "lpad",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.substring" -> "substring",
      "option.length" -> "5",
      "option.pad" -> "pad"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "ltrim",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "regexp_extract",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.regex" -> "regex",
      "option.groupIndex" -> "2"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "unbase64",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "rpad",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.length" -> "5",
      "option.pad" -> "pad"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "repeat",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.repeatNumber" -> "3"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "rtrim",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.trimString" -> "trimString"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "split",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.regex" -> "regex",
      "option.limit" -> "2"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "substring",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.position" -> "2",
      "option.length" -> "2"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "substring_index",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b",
      "option.delimiter" -> "delimiter",
      "option.count" -> "5"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "trim",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b"
    ),
    Map(
      "operation" -> OperationType.STRING.toString,
      "function" -> "upper",
      "option.sourceColumn" -> "a",
      "option.targetColumn" -> "b"
    )
  )

  it("validate") {
    for (config <- configs) {
      val result = StringStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(config)
      result should be(true)
    }
  }

  it("convert") {
    for (config <- configs) {
      val nodeConfig = Node("id", config)
      val result = StringStageBuilder invokePrivate PrivateMethod[Stage]('convert)(nodeConfig)
      result.getClass should be(classOf[StringStage])
    }
  }
}