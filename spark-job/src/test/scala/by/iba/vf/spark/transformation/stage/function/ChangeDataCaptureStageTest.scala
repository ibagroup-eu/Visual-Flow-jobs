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

class ChangeDataCaptureStageTest extends AnyFunSpec with MockitoSugar with PrivateMethodTester {
  implicit val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._

  it("process") {
    val newDF = Seq((1, 2), (5, 6), (8, 9)).toDF("a", "b")
    val oldDF = Seq((1, 2), (3, 4), (5, 7)).toDF("a", "b")
    val expected = Seq((3, 4, 2), (5, 6, 3), (8, 9, 1)).toDF("a", "b", "operation")
    val stage = new ChangeDataCaptureStage(Node("id", Map()), Seq("a"), "1", "2", false)

    val result = stage invokePrivate PrivateMethod[Option[DataFrame]]('process)(Map("1" -> newDF, "2" -> oldDF), spark)

    result.orNull.collectAsList() should be(expected.collectAsList())
  }

  it("processRetAll") {
    val newDF = Seq((1, 2), (5, 6), (8, 9)).toDF("a", "b")
    val oldDF = Seq((1, 2), (3, 4), (5, 7)).toDF("a", "b")
    val expected = Seq((1, 2, 0), (3, 4, 2), (5, 6, 3), (8, 9, 1)).toDF("a", "b", "operation")
    val stage = new ChangeDataCaptureStage(Node("id", Map()), Seq("a"), "1", "2", true)

    val result = stage invokePrivate PrivateMethod[Option[DataFrame]]('process)(Map("1" -> newDF, "2" -> oldDF), spark)

    result.orNull.collectAsList() should be(expected.collectAsList())
  }
}

class ChangeDataCaptureStageBuilderTest extends AnyFunSpec with PrivateMethodTester {
  it("validate") {
    val config: Map[String, String] = Map("operation" -> OperationType.CDC.toString, "keyColumns" -> "testing", "newDataset" -> "1", "oldDataset" -> "2", "mode" -> "delta")

    val result = ChangeDataCaptureStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(config)
    result should be(true)
  }

  it("convert") {
    val config: Node = Node("id", Map("operation" -> OperationType.CDC.toString, "keyColumns" -> "testing", "newDataset" -> "1", "oldDataset" -> "2", "mode" -> "delta"))

    val result = ChangeDataCaptureStageBuilder invokePrivate PrivateMethod[Stage]('convert)(config)

    result.getClass should be(classOf[ChangeDataCaptureStage])
  }
}
