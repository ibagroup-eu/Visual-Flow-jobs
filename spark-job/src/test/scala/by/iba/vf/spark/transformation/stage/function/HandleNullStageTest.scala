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
import org.apache.spark.sql.{DataFrame, DataFrameNaFunctions, SparkSession}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers._

class HandleNullStageTest extends AnyFunSpec with MockitoSugar with PrivateMethodTester {
  it("process") {
    implicit lazy val spark: SparkSession = mock[SparkSession]
    val df = mock[DataFrame]
    val df1 = mock[DataFrameNaFunctions]
    val df2 = mock[DataFrame]

    val configs = List(
      Map("fillValues" -> "5,7",
        "fillValueType" -> "custom",
        "fillColumns" -> "a,b"),
      Map("fillValues" -> "5",
        "fillValueType" -> "custom"),
      Map("dropType" -> "row",
          "dropChoice" -> "any")
    )

    for (config <- configs) {
      if (config.contains("fillColumns") && config.contains("fillValues")) {
        val fillMap = (config("fillColumns").split(",") zip config("fillValues").split(",")).toMap
        when(df.na).thenReturn(df1)
        when(df1.fill(fillMap)).thenReturn(df2)

        val stage = new HandleNullStage("id", HandleNullType.FILL, config)

        val result = stage invokePrivate PrivateMethod[Option[DataFrame]]('process)(Map("1" -> df), spark)

        result should be(Some(df2))
      } else if (config.contains("fillValues") && !config.contains("fillColumns")) {
        when(df.na).thenReturn(df1)
        when(df1.fill(config("fillValues"))).thenReturn(df2)

        val stage = new HandleNullStage("id", HandleNullType.FILL, config)
        val result = stage invokePrivate PrivateMethod[Option[DataFrame]]('process)(Map("1" -> df), spark)
        result should be(Some(df2))
      } else if (config.contains("dropType") && config("dropType") == "row") {
        when(df.na).thenReturn(df1)
        when(df1.drop(config("dropChoice"))).thenReturn(df2)

        val stage = new HandleNullStage("id", HandleNullType.DROP, config)
        val result = stage invokePrivate PrivateMethod[Option[DataFrame]]('process)(Map("1" -> df), spark)
        result should be(Some(df2))
      }
    }

  }

  it("handleNull") {
    val df = mock[DataFrame]
    val df1 = mock[DataFrameNaFunctions]
    val df2 = mock[DataFrame]

    val configs = List(
      Map("fillValues" -> "5,7",
        "fillValueType" -> "custom",
        "fillColumns" -> "a,b"),
      Map("fillValues" -> "5",
        "fillValueType" -> "custom"),
      Map("dropType" -> "row",
        "dropChoice" -> "any")
    )

    for (config <- configs) {
      if (config.contains("fillColumns") && config.contains("fillValues")) {
        val fillMap = (config("fillColumns").split(",") zip config("fillValues").split(",")).toMap
        when(df.na).thenReturn(df1)
        when(df1.fill(fillMap)).thenReturn(df2)

        val stage = new HandleNullStage("id", HandleNullType.FILL, config)

        val result = stage.fill(df)

        result should be(df2)
      } else if (config.contains("fillValues") && !config.contains("fillColumns")) {
        when(df.na).thenReturn(df1)
        when(df1.fill(config("fillValues"))).thenReturn(df2)

        val stage = new HandleNullStage("id", HandleNullType.FILL, config)

        val result = stage.fill(df)

        result should be(df2)
      } else if (config.contains("dropType") && config("dropType") == "row") {
        when(df.na).thenReturn(df1)
        when(df1.drop(config("dropChoice"))).thenReturn(df2)

        val stage = new HandleNullStage("id", HandleNullType.DROP, config)
        val result = stage.drop(df)
        result should be(df2)
      }
    }
  }
}

class HandleNullStageTestBuilderTest extends AnyFunSpec with PrivateMethodTester {
  it("validate") {
    val configs = List(
      Map(
        "operation" -> OperationType.HANDLE_NULL.toString,
        "mode" -> "fill",
        "option.fillValues" -> "5,7",
        "option.fillValueType" -> "custom",
        "option.fillColumns" -> "a,b"
      ),
      Map(
        "operation" -> OperationType.HANDLE_NULL.toString,
        "mode" -> "fill",
        "option.fillValues" -> "some text",
        "option.fillValueType" -> "custom"
      ),
      Map(
        "operation" -> OperationType.HANDLE_NULL.toString,
        "mode" -> "fill",
        "option.fillValueType" -> "agg",
        "option.fillColumns" -> "b",
        "option.fillStrategy" -> "mean"
      ),
      Map(
        "operation" -> OperationType.HANDLE_NULL.toString,
        "mode" -> "drop",
        "option.dropType" -> "row",
        "option.dropChoice" -> "any"
      ),
      Map(
        "operation" -> OperationType.HANDLE_NULL.toString,
        "mode" -> "drop",
        "option.dropType" -> "column",
        "option.dropChoice" -> "names",
        "option.dropColumns" -> "id,name"
      ),
      Map(
        "operation" -> OperationType.HANDLE_NULL.toString,
        "mode" -> "drop",
        "option.dropType" -> "row",
        "option.dropChoice" -> "all"
      ),
      Map(
        "operation" -> OperationType.HANDLE_NULL.toString,
        "mode" -> "drop",
        "option.dropType" -> "row",
        "option.dropChoice" -> "names",
        "option.dropColumns" -> "id,name"
      )
    )

    for (config <- configs) {
      val result = HandleNullStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(config)
      result should be(true)
    }
  }

  it("convert") {
    val configs = List(
      Node("id", Map(
        "operation" -> OperationType.HANDLE_NULL.toString,
        "mode" -> "fill",
        "option.fillValues" -> "5,7",
        "option.fillValueType" -> "custom",
        "option.fillColumns" -> "a,b")),
      Node("id", Map(
        "operation" -> OperationType.HANDLE_NULL.toString,
        "mode" -> "fill",
        "option.fillValues" -> "some text",
        "option.fillValueType" -> "custom"
      )),
      Node("id", Map(
        "operation" -> OperationType.HANDLE_NULL.toString,
        "mode" -> "fill",
        "option.fillValueType" -> "agg",
        "option.fillColumns" -> "b",
        "option.fillStrategy" -> "mean"
      )),
      Node("id", Map(
        "operation" -> OperationType.HANDLE_NULL.toString,
        "mode" -> "drop",
        "option.dropType" -> "row",
        "option.dropChoice" -> "any"
      )),
      Node("id", Map(
        "operation" -> OperationType.HANDLE_NULL.toString,
        "mode" -> "drop",
        "option.dropType" -> "column",
        "option.dropChoice" -> "names",
        "option.dropColumns" -> "id,name"
      )),
      Node("id", Map(
        "operation" -> OperationType.HANDLE_NULL.toString,
        "mode" -> "drop",
        "option.dropType" -> "row",
        "option.dropChoice" -> "all"
      )),
      Node("id", Map(
        "operation" -> OperationType.HANDLE_NULL.toString,
        "mode" -> "drop",
        "option.dropType" -> "row",
        "option.dropChoice" -> "names",
        "option.dropColumns" -> "id,name"
      ))
    )

    for (config <- configs) {
      val result = HandleNullStageBuilder invokePrivate PrivateMethod[Stage]('convert)(config)
      result.getClass should be(classOf[HandleNullStage])
    }
  }
}