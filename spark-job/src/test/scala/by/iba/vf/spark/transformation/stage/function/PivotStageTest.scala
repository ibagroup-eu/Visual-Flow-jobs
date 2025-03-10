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
import org.apache.spark.sql.{DataFrame, SparkSession, RelationalGroupedDataset}
import org.apache.spark.sql.functions.{col, expr}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers._

class PivotStageTest extends AnyFunSpec with MockitoSugar with PrivateMethodTester {
  it("process") {
    implicit lazy val spark: SparkSession = mock[SparkSession]
    val df = mock[DataFrame]
    val df2 = mock[DataFrame]

    for (operationType <- Seq("pivot", "unpivot")) {
      if (operationType == "pivot") {
        val groupedDF = mock[RelationalGroupedDataset]
        val pivotedDF = mock[RelationalGroupedDataset]
        when(df.groupBy(col("a"))).thenReturn(groupedDF)
        when(groupedDF.pivot(col("c"))).thenReturn(pivotedDF)
        when(pivotedDF.agg(expr("sum(d)"))).thenReturn(df2)

        val stage = new PivotStage(Node("id", Map()), "pivot", Map(
          "groupBy" -> "a",
          "pivotColumn" -> "c",
          "aggregation" -> "sum(d)")
        )

        val result = stage invokePrivate PrivateMethod[Option[DataFrame]]('process)(Map("1" -> df), spark)

        result should be(Some(df2))
      } else {
        val df1 = mock[DataFrame]
        when(df.selectExpr(Array("a", "stack(3, 'b1', b1, 'b2', b2, 'b3', b3) as (c1, c2)"): _*)).thenReturn(df1)
        when(df1.where("c2 is not null")).thenReturn(df2)

        val stage = new PivotStage(Node("id", Map()), "unpivot", Map(
          "unchangedColumns" -> "a",
          "unpivotColumns" -> "b1,b2,b3",
          "unpivotNames" -> "c1,c2"
        ))

        val result = stage invokePrivate PrivateMethod[Option[DataFrame]]('process)(Map("1" -> df), spark)

        result should be(Some(df2))
      }
    }

  }

  it("pivot") {
    val df = mock[DataFrame]
    val df2 = mock[DataFrame]

    for (operationType <- Seq("pivot", "unpivot")) {
      if (operationType == "pivot") {
        val groupedDF = mock[RelationalGroupedDataset]
        val pivotedDF = mock[RelationalGroupedDataset]
        when(df.groupBy(col("a"))).thenReturn(groupedDF)
        when(groupedDF.pivot(col("c"))).thenReturn(pivotedDF)
        when(pivotedDF.agg(expr("sum(d)"))).thenReturn(df2)

        val stage = new PivotStage(Node("id", Map()), operationType, Map(
          "groupBy" -> "a",
          "pivotColumn" -> "c",
          "aggregation" -> "sum(d)")
        )

        val result = stage.pivot(df)

        result should be(df2)
      } else {
        val df1 = mock[DataFrame]
        when(df.selectExpr(Array("a", "stack(3, 'b1', b1, 'b2', b2, 'b3', b3) as (c1, c2)"): _*)).thenReturn(df1)
        when(df1.where("c2 is not null")).thenReturn(df2)

        val stage = new PivotStage(Node("id", Map()), operationType, Map(
          "unchangedColumns" -> "a",
          "unpivotColumns" -> "b1,b2,b3",
          "unpivotNames" -> "c1,c2"
        ))

        val result = stage.pivot(df)

        result should be(df2)
      }
    }
  }
}

class PivotStageTestBuilderTest extends AnyFunSpec with PrivateMethodTester {
  it("validate") {
    for (operationType <- Seq("pivot", "unpivot")) {
      if (operationType == "pivot") {
        val config: Map[String, String] = Map(
          "operation" -> OperationType.PIVOT.toString,
          "operationType" -> operationType,
          "option.groupBy" -> "a",
          "option.pivotColumn" -> "c",
          "option.aggregation" -> "sum(d)"
        )

        val result = PivotStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(config)
        result should be(true)
      } else {
        val config: Map[String, String] = Map(
          "operation" -> OperationType.PIVOT.toString,
          "operationType" -> operationType,
          "option.unchangedColumns" -> "a",
          "option.unpivotColumns" -> "b1,b2,b3",
          "option.unpivotNames" -> "c1,c2"
        )

        val result = PivotStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(config)
        result should be(true)
      }
    }
  }

  it("convert") {
    for (operationType <- Seq("pivot", "unpivot")) {
      if (operationType == "pivot") {
        val config: Node = Node("id", Map(
          "operation" -> OperationType.PIVOT.toString,
          "operationType" -> operationType,
          "option.groupBy" -> "a",
          "option.pivotColumn" -> "c",
          "option.aggregation" -> "sum(d)")
        )

        val result = PivotStageBuilder invokePrivate PrivateMethod[Stage]('convert)(config)

        result.getClass should be(classOf[PivotStage])
      } else {
        val config: Node = Node("id", Map(
          "operation" -> OperationType.PIVOT.toString,
          "operationType" -> operationType,
          "option.unchangedColumns" -> "a",
          "option.unpivotColumns" -> "b1,b2,b3",
          "option.unpivotNames" -> "c1,c2")
        )

        val result = PivotStageBuilder invokePrivate PrivateMethod[Stage]('convert)(config)

        result.getClass should be(classOf[PivotStage])
      }
    }
  }
}
