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
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers._

class TransformStageTest extends AnyFunSpec with MockitoSugar with PrivateMethodTester {
  it("process") {
    implicit lazy val spark: SparkSession = mock[SparkSession]
    val df = mock[DataFrame]
    val df2 = mock[DataFrame]
    doNothing.when(df).createOrReplaceTempView("tmpTable")

    val selectStmt = "a+b,c as d"

    when(spark.sql(s"select $selectStmt from tmpTable")).thenReturn(df2)
    val stage = new TransformStage("id", selectStmt, TransformationMode.Simple, None)

    val result = stage invokePrivate PrivateMethod[Option[DataFrame]]('process)(Map("1" -> df), spark)

    result should be(Some(df2))
  }

  it("processFullSql") {
    implicit lazy val spark: SparkSession = mock[SparkSession]
    val df = mock[DataFrame]
    val df2 = mock[DataFrame]
    val tableName = "testTable"

    doNothing.when(df).createOrReplaceTempView(tableName)

    val selectStmt = "select * from testTable order by id"

    when(spark.sql(selectStmt)).thenReturn(df2)
    val stage = new TransformStage("id", selectStmt, TransformationMode.Full_SQL, Some(tableName))

    val result = stage invokePrivate PrivateMethod[Option[DataFrame]]('process)(Map("1" -> df), spark)

    result should be(Some(df2))
  }
}

class TransformStageBuilderTest extends AnyFunSpec with PrivateMethodTester {
  it("validate") {
    val config: Map[String, String] = Map("operation" -> OperationType.TRANSFORM.toString, "statement" -> "testing")
    val configSimple: Map[String, String] = Map("operation" -> OperationType.TRANSFORM.toString, "statement" -> "testing", "mode" -> TransformationMode.Simple.toString)

    val result = TransformStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(config)
    result should be(true)
    TransformStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(configSimple) should be(true)
  }

  it("validateFullSql") {
    val config: Map[String, String] = Map("operation" -> OperationType.TRANSFORM.toString, "statement" -> "testing", "mode" -> TransformationMode.Full_SQL.toString, "tableName" -> "test")

    val result = TransformStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(config)
    result should be(true)
  }

  it("convert") {
    val config: Node = Node("id", Map("operation" -> OperationType.TRANSFORM.toString, "statement" -> "testing"))

    val result = TransformStageBuilder invokePrivate PrivateMethod[Stage]('convert)(config)

    result.getClass should be(classOf[TransformStage])
  }

  it("convertFullSql") {
    val config: Node = Node("id", Map("operation" -> OperationType.TRANSFORM.toString, "statement" -> "testing", "mode" -> TransformationMode.Full_SQL.toString, "tableName" -> "test"))

    val result = TransformStageBuilder invokePrivate PrivateMethod[Stage]('convert)(config)

    result.getClass should be(classOf[TransformStage])
  }
}
