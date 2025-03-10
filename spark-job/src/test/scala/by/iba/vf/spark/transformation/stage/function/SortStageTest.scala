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
import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers._

class SortStageTest extends AnyFunSpec with MockitoSugar with PrivateMethodTester {
  it("process") {
    implicit lazy val spark: SparkSession = mock[SparkSession]
    val df = mock[DataFrame]
    val df2 = mock[DataFrame]
    when(df.orderBy(asc("b"), desc("a"))).thenReturn(df2)
    val stage = new SortStage(Node("id", Map()), "fullSort", Seq(asc("b"), desc("a")))

    val result = stage invokePrivate PrivateMethod[Option[DataFrame]]('process)(Map("1" -> df), spark)

    result should be(Some(df2))
  }

  it("sort") {
    val df = mock[DataFrame]
    val df2 = mock[DataFrame]
    when(df.orderBy(asc("b"), desc("a"))).thenReturn(df2)
    val stage = new SortStage(Node("id", Map()), "fullSort", Seq(asc("b"), desc("a")))

    val result = stage.sort(df)

    result should be(df2)
  }
}

class SortStageTestBuilderTest extends AnyFunSpec with PrivateMethodTester {
  it("validate") {
    val config: Map[String, String] = Map(
      "operation" -> OperationType.SORT.toString,
      "sortType" -> "fullSort",
      "orderColumns" -> "b:asc, a:desc"
    )

    val result = SortStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(config)
    result should be(true)
  }

  it("convert") {
    val config: Node = Node("id", Map(
      "operation" -> OperationType.SORT.toString,
      "sortType" -> "fullSort",
      "orderColumns" -> "b:asc, a:desc")
    )

    val result = SortStageBuilder invokePrivate PrivateMethod[Stage]('convert)(config)

    result.getClass should be(classOf[SortStage])
  }
}
