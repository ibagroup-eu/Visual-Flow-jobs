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
import org.apache.spark.sql.SparkSession
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers._

class JoinStageTest extends AnyFunSpec with MockitoSugar with PrivateMethodTester {
  it("process") {
    implicit lazy val spark: SparkSession = mock[SparkSession]
    val fields = Seq("test1", "test2")
    val joinType = "inner"
    val df = mock[DataFrame]
    val df2 = mock[DataFrame]
    val df3 = mock[DataFrame]
    when(df.as("left")).thenReturn(df)
    when(df2.as("right")).thenReturn(df2)
    when(df.join(df2, fields, joinType)).thenReturn(df3)
    val stage = new JoinStage("id", joinType, fields, "1", "2")

    val result = stage invokePrivate PrivateMethod[Option[DataFrame]]('process)(Map("1" -> df, "2" -> df2), spark)

    result should be(Some(df3))
  }

  it("join") {
    val fields = Seq("test1", "test2")
    val joinType = "inner"
    val df = mock[DataFrame]
    val df2 = mock[DataFrame]
    val df3 = mock[DataFrame]
    when(df.as("left")).thenReturn(df)
    when(df2.as("right")).thenReturn(df2)
    when(df.join(df2, fields, joinType)).thenReturn(df3)
    val stage = new JoinStage("id", joinType, fields, "1", "2")

    val result = stage invokePrivate PrivateMethod[DataFrame]('join)(df, df2)

    result should be(df3)
  }
}

class JoinStageBuilderTest extends AnyFunSpec with PrivateMethodTester {
  it("validate") {
    val config: Map[String, String] =
      Map(
        "operation" -> OperationType.JOIN.toString,
        "joinType" -> "inner",
        "columns" -> "test",
        "leftDataset" -> "1",
        "rightDataset" -> "2"
      )

    val result = JoinStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(config)
    result should be(true)
  }

  it("convert") {
    val config: Node =
      Node(
        "id",
        Map(
          "operation" -> OperationType.JOIN.toString,
          "joinType" -> "inner",
          "columns" -> "test",
          "leftDataset" -> "1",
          "rightDataset" -> "2"
        )
      )

    val result = JoinStageBuilder invokePrivate PrivateMethod[Stage]('convert)(config)

    result.getClass should be(classOf[JoinStage])
  }
}
