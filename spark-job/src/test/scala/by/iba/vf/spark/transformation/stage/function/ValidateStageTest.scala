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
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers._

class ValidateStageTest extends AnyFunSpec with MockitoSugar with PrivateMethodTester {
  it("process") {
    implicit lazy val spark: SparkSession = mock[SparkSession]
    val df = mock[DataFrame]
    val df2 = mock[DataFrame]
    val schema =  StructType(Array(StructField("a",StringType,true)))
    val schema2 =  StructType(Array(StructField("a",IntegerType,true)))

    val funcVector = Vector(
      "string",
      "a < 1",
      "a > 5",
      "LENGTH(a) < 1",
      "LENGTH(a) > 5",
      "1,2,3",
      "[A-Z]",
      "a IS NULL",
      ""
    )

    val operationVector = Vector(
      "dataType",
      "minValue",
      "maxValue",
      "minLength",
      "maxLength",
      "inValues",
      "regex",
      "notNull",
      "uniqueness"
    )

    val mapVector = Vector(
      List(Map("column" -> "a", "validations" -> "[{\"type\":\"dataType\", \"dataType\": \"string\"}]")),
      List(Map("column" -> "a", "validations" -> "[{\"type\": \"minValue\", \"minValue\": \"1\"}]")),
      List(Map("column" -> "a", "validations" -> "[{\"type\": \"maxValue\", \"maxValue\": \"5\"}]")),
      List(Map("column" -> "a", "validations" -> "[{\"type\": \"minLength\", \"minLength\": \"1\"}]")),
      List(Map("column" -> "a", "validations" -> "[{\"type\": \"maxLength\", \"maxLength\": \"5\"}]")),
      List(Map("column" -> "a", "validations" -> "[{\"type\": \"inValues\", \"inValues\": \"1,2,3\"}]")),
      List(Map("column" -> "a", "validations" -> "[{\"type\": \"regex\", \"regex\": \"[A-Z]\"}]")),
      List(Map("column" -> "a", "validations" -> "[{\"type\": \"notNull\"}]")),
      List(Map("column" -> "a", "validations" -> "[{\"type\": \"uniqueness\"}]"))
    )

    //check all functions for one column
    for (i <- operationVector.indices; j <- 0 to 1) {
      val res_df = if (j == 0) df else df2
      val res_schema = if (j == 0) schema else schema2
      if (Seq("minValue", "maxValue", "minLength", "maxLength", "notNull").contains(operationVector(i))) {
        when(df.filter(funcVector(i))).thenReturn(res_df)
        when(df.filter(funcVector(i)).count()).thenReturn(j)
      } else if (operationVector(i) == "dataType") {
        when(df.select("a")).thenReturn(res_df)
        when(df.select("a").schema).thenReturn(res_schema)
      } else if (operationVector(i) == "regex") {
        when(df.filter(col("a").rlike(funcVector(i)))).thenReturn(res_df)
      } else if (operationVector(i) == "inValues") {
        when(df.filter(!col("a").isin(funcVector(i).split(','):_*))).thenReturn(res_df)
      } else if (operationVector(i) == "uniqueness") {
        when(df.dropDuplicates("a".split(","))).thenReturn(res_df)
      }

      val stage = new ValidateStage("id", mapVector(i), false)
      val result = stage invokePrivate PrivateMethod[Option[DataFrame]]('process)(Map("1" -> df), spark)
      result should be (Some(df))
    }

    //check uniqueness for multiply columns
    for (j <- 0 to 1) {
      val res_df = if (j == 0) df else df2
      when(df.dropDuplicates("a,b".split(","))).thenReturn(res_df)

      val stage = new ValidateStage("id", List(Map("column" -> "a,b", "validations" -> "[{\"type\": \"uniqueness\"}]")), false)
      val result = stage invokePrivate PrivateMethod[Option[DataFrame]]('process)(Map("1" -> df), spark)
      result should be (Some(df))
    }
  }

  it("validate") {
    val df = mock[DataFrame]
    val df2 = mock[DataFrame]
    val schema =  StructType(Array(StructField("a",StringType,true)))
    val schema2 =  StructType(Array(StructField("a",IntegerType,true)))

    val funcVector = Vector(
      "string",
      "a < 1",
      "a > 5",
      "LENGTH(a) < 1",
      "LENGTH(a) > 5",
      "1,2,3",
      "[A-Z]",
      "a IS NULL",
      ""
    )

    val operationVector = Vector(
      "dataType",
      "minValue",
      "maxValue",
      "minLength",
      "maxLength",
      "inValues",
      "regex",
      "notNull",
      "uniqueness"
    )

    val mapVector = Vector(
      List(Map("column" -> "a", "validations" -> "[{\"type\":\"dataType\", \"dataType\": \"string\"}]")),
      List(Map("column" -> "a", "validations" -> "[{\"type\": \"minValue\", \"minValue\": \"1\"}]")),
      List(Map("column" -> "a", "validations" -> "[{\"type\": \"maxValue\", \"maxValue\": \"5\"}]")),
      List(Map("column" -> "a", "validations" -> "[{\"type\": \"minLength\", \"minLength\": \"1\"}]")),
      List(Map("column" -> "a", "validations" -> "[{\"type\": \"maxLength\", \"maxLength\": \"5\"}]")),
      List(Map("column" -> "a", "validations" -> "[{\"type\": \"inValues\", \"inValues\": \"1,2,3\"}]")),
      List(Map("column" -> "a", "validations" -> "[{\"type\": \"regex\", \"regex\": \"[A-Z]\"}]")),
      List(Map("column" -> "a", "validations" -> "[{\"type\": \"notNull\"}]")),
      List(Map("column" -> "a", "validations" -> "[{\"type\": \"uniqueness\"}]"))
    )

    //check all functions for one column
    for (i <- operationVector.indices; j <- 0 to 1) {
      val res_df = if (j == 0) df else df2
      val res_schema = if (j == 0) schema else schema2
      if (Seq("minValue", "maxValue", "minLength", "maxLength", "notNull").contains(operationVector(i))) {
        when(df.filter(funcVector(i))).thenReturn(res_df)
        when(df.filter(funcVector(i)).count()).thenReturn(j)
      } else if (operationVector(i) == "dataType") {
        when(df.select("a")).thenReturn(res_df)
        when(df.select("a").schema).thenReturn(res_schema)
      } else if (operationVector(i) == "regex") {
        when(df.filter(col("a").rlike(funcVector(i)))).thenReturn(res_df)
      } else if (operationVector(i) == "inValues") {
        when(df.filter(!col("a").isin(funcVector(i).split(','):_*))).thenReturn(res_df)
      } else if (operationVector(i) == "uniqueness") {
        when(df.dropDuplicates("a".split(","))).thenReturn(res_df)
      }

      val stage = new ValidateStage("id", mapVector(i), false)
      val result = operationVector(i) match {
        case "dataType" => stage.dataType(df, "a", "string")
        case "minValue" => stage.minValue(df, "a", "1")
        case "maxValue" => stage.maxValue(df, "a","5")
        case "minLength" => stage.minLength(df, "a","1")
        case "maxLength" => stage.maxLength(df, "a", "5")
        case "inValues" => stage.inValues(df, "a", "1,2,3")
        case "regex" => stage.regex(df, "a", "[A-Z]")
        case "notNull" => stage.notNull(df, "a")
        case "uniqueness" => stage.uniqueness(df, "a")
      }

      result should be ()
    }

    //check uniqueness for multiply columns
    for (j <- 0 to 1) {
      val res_df = if (j == 0) df else df2
      when(df.dropDuplicates("a,b".split(","))).thenReturn(res_df)

      val stage = new ValidateStage("id", List(Map("column" -> "a,b", "validations" -> "[{\"type\": \"uniqueness\"}]")), false)
      val result = stage.uniqueness(df, "a,b")
      result should be ()
    }
  }
}

class ValidateStageBuilderTest extends AnyFunSpec with PrivateMethodTester {
  it("validate") {
    val config: Map[String, String] = Map(
      "operation" -> OperationType.VALIDATE.toString,
      "validateConfig" -> "[{\"column\": \"a\",\"validations\":\"[{\\\"type\\\":\\\"dataType\\\",\\\"dataType\\\":\\\"integer\\\"}\"}]"
    )

    val result = ValidateStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(config)
    result should be(true)
  }

  it("convert") {
    val config: Node = Node("validate1", Map(
      "operation" -> OperationType.VALIDATE.toString,
      "validateConfig" -> "[{\"column\": \"a\",\"validations\":\"[{\\\"type\\\":\\\"dataType\\\",\\\"dataType\\\":\\\"integer\\\"}\"}]"
    ))

    val result = ValidateStageBuilder invokePrivate PrivateMethod[Stage]('convert)(config)

    result.getClass should be(classOf[ValidateStage])
  }
}

