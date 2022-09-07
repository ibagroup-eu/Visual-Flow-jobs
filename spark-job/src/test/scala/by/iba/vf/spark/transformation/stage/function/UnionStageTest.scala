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
import by.iba.vf.spark.transformation.exception.TransformationConfigurationException
import by.iba.vf.spark.transformation.stage.OperationType
import by.iba.vf.spark.transformation.stage.Stage
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers._

class UnionStageTest extends AnyFunSpec with MockitoSugar with PrivateMethodTester {
  it("process") {
    implicit lazy val spark: SparkSession = mock[SparkSession]
    val unionAll = true
    val df = mock[DataFrame]
    val df2 = mock[DataFrame]
    val df3 = mock[DataFrame]
    when(df.union(df2)).thenReturn(df3)
    val stage = new UnionStage("id", unionAll)

    val result = stage invokePrivate PrivateMethod[Option[DataFrame]]('process)(Map("1" -> df, "2" -> df2), spark)

    result should be(Some(df3))
  }

  it("union") {
    val unionAll = false
    val df = mock[DataFrame]
    val df1 = mock[DataFrame]
    val df2 = mock[DataFrame]
    val df3 = mock[DataFrame]
    when(df.union(df1)).thenReturn(df2)
    when(df2.distinct()).thenReturn(df3)
    val stage = new UnionStage("id", unionAll)

    val result = stage invokePrivate PrivateMethod[DataFrame]('union)(df, df1)

    result should be(df3)
  }
}

class UnionStageBuilderTest extends AnyFunSpec with PrivateMethodTester {
  describe("validate") {
    it("unionAll") {
      val config: Map[String, String] =
        Map("operation" -> OperationType.UNION.toString, "type" -> "all")

      val result = UnionStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(config)
      result should be(true)
    }

    it("notAll") {
      val config: Map[String, String] =
        Map("operation" -> OperationType.UNION.toString, "type" -> "unionNotAll")

      val result = UnionStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(config)
      result should be(true)
    }

    it("notSet") {
      val config: Map[String, String] =
        Map("operation" -> OperationType.UNION.toString)

      val result = UnionStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(config)
      result should be(false)
    }
  }

  describe("convert") {
    it("distinct") {
      val config: Node =
        Node("id", Map("operation" -> OperationType.UNION.toString, "type" -> "distinct"))

      val result = UnionStageBuilder invokePrivate PrivateMethod[Stage]('convert)(config)

      result.getClass should be(classOf[UnionStage])
    }

    it("all") {
      val config: Node =
        Node("id", Map("operation" -> OperationType.UNION.toString, "type" -> "all"))

      val result = UnionStageBuilder invokePrivate PrivateMethod[Stage]('convert)(config)

      result.getClass should be(classOf[UnionStage])
    }

    it("invalid") {
      val config: Node =
        Node("id", Map("operation" -> OperationType.UNION.toString, "type" -> "321"))

      the[TransformationConfigurationException] thrownBy (UnionStageBuilder invokePrivate PrivateMethod[Stage](
        'convert
      )(config))
    }
  }
}
