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
import by.iba.vf.spark.transformation.stage.OperationType
import by.iba.vf.spark.transformation.stage.Stage
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.RelationalGroupedDataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers._

class GroupByStageTest extends AnyFunSpec with MockitoSugar with PrivateMethodTester {
  it("process") {
    implicit lazy val spark: SparkSession = mock[SparkSession]
    val df = mock[DataFrame]
    val aggDF = mock[DataFrame]
    val df2 = mock[DataFrame]
    val groupingCols = Array("test1", "test2")
    val colFun = Array("test1" -> "max", "test2" -> "max", "test3" -> "max")
    val oldNewNames = Map("max(test1)" -> "max_test1")
    val groupedDF = mock[RelationalGroupedDataset]

    when(df.groupBy(groupingCols.map(col): _*)).thenReturn(groupedDF)
    when(groupedDF.agg(colFun.head, colFun.tail: _*)).thenReturn(aggDF)
    when(oldNewNames.foldLeft(aggDF){
      case (tempDF, (oldName, newName)) => tempDF.withColumnRenamed(oldName, newName)
    }).thenReturn(df2)

    val stage = new GroupByStage(Node("id", Map()), groupingCols, colFun, oldNewNames, false)

    val result = stage invokePrivate PrivateMethod[Option[DataFrame]]('process)(Map("1" -> df), spark)

    result should be(Some(df2))
  }

  it("groupBy") {
    val df = mock[DataFrame]
    val aggDF = mock[DataFrame]
    val df2 = mock[DataFrame]
    val groupingCols = Array("test1", "test2")
    val colFun = Array("test1" -> "max", "test2" -> "max", "test3" -> "max", "*" -> "count")
    val oldNewNames = Map("max(test1)" -> "max_test1")
    val groupedDF = mock[RelationalGroupedDataset]
    val stage = new GroupByStage(Node("id", Map()), groupingCols, colFun, oldNewNames, true)

    when(df.groupBy(groupingCols.map(col): _*)).thenReturn(groupedDF)
    when(groupedDF.agg(colFun.head, colFun.tail: _*)).thenReturn(aggDF)
    when(oldNewNames.foldLeft(aggDF){
      case (tempDF, (oldName, newName)) => tempDF.withColumnRenamed(oldName, newName)
    }).thenReturn(df2)
    when(df2.drop(groupingCols: _*)).thenReturn(df2)

    val result = stage.groupBy(df)

    result should be(df2)
    verify(df).groupBy(groupingCols.map(col): _*)
    verify(groupedDF).agg(colFun.head, colFun.tail: _*)
    verify(df2).drop(groupingCols: _*)
  }

}

class GroupByStageBuilderTest extends AnyFunSpec with PrivateMethodTester {
  it("validate") {
    val config: Map[String, String] =
      Map(
        "operation" -> OperationType.GROUP.toString,
        "groupingCriteria" -> "salary:avg:avg_salary,test:max:,*:count",
        "groupingColumns" -> "a"
      )

    val result = GroupByStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(config)
    result should be(true)
  }

  it("convert") {
    val config: Node =
      Node(
        "id",
        Map(
          "operation" -> OperationType.GROUP.toString,
          "groupingCriteria" -> "salary:avg:avg_salary,test:max:,*:count",
          "groupingColumns" -> "a"
        )
      )

    val result = GroupByStageBuilder invokePrivate PrivateMethod[Stage]('convert)(config)

    result.getClass should be(classOf[GroupByStage])
  }
}
