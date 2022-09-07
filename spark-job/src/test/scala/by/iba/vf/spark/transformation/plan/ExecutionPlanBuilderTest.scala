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
package by.iba.vf.spark.transformation.plan

import by.iba.vf.spark.transformation.config.Edge
import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.config.ProcessConfiguration
import by.iba.vf.spark.transformation.exception.InvalidStageInputException
import by.iba.vf.spark.transformation.exception.UnknownStageException
import org.mockito.scalatest.MockitoSugar
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers._

class ExecutionPlanBuilderTest extends AnyFunSpec with MockitoSugar {

  describe("buildExecutionPlan") {

    it("success") {
      val pc = ProcessConfiguration(
        List(
          Node("gen-table", Map("operation" -> "READ", "storage" -> "GENERATOR", "alias" -> "left")),
          Node("print", Map("operation" -> "WRITE", "storage" -> "STDOUT"))
        ),
        List(Edge("gen-table", "print"))
      )
      val result = ExecutionPlanBuilder.buildExecutionPlan(pc)
      val expectedDepMap = Map("gen-table" -> List(), "print" -> List("gen-table"))
      result.dependencyMap should be(expectedDepMap)
    }

    it("unknown stage") {
      val pc = ProcessConfiguration(
        List(
          Node("gen-table", Map("operation" -> "TEST", "storage" -> "GENERATOR", "alias" -> "left")),
          Node("print", Map("operation" -> "WRITE", "storage" -> "STDOUT"))
        ),
        List(Edge("gen-table", "print"))
      )

      the[UnknownStageException] thrownBy ExecutionPlanBuilder.buildExecutionPlan(pc)
    }

    it("invalid stage input") {
      val pc = ProcessConfiguration(
        List(
          Node("gen-table", Map("operation" -> "READ", "storage" -> "GENERATOR", "alias" -> "left")),
          Node("print", Map("operation" -> "WRITE", "storage" -> "STDOUT"))
        ),
        List(Edge("gen-table", "print"), Edge("gen-table", "print"))
      )

      the[InvalidStageInputException] thrownBy ExecutionPlanBuilder.buildExecutionPlan(pc)
    }
// TODO: make this test work
//    it("node not found") {
//      val pc = ProcessConfiguration(List(
//        Node("gen-table", Map("operation" -> "READ", "storage" -> "GENERATOR", "alias" -> "left")),
//        Node("join-data", Map("operation" -> "JOIN", "joinType" -> "inner", "fields" -> "a"))),
//        List(Edge("gen-table", "join-data"), Edge("qwe", "join-data")))
//      print(ExecutionPlanBuilder.buildExecutionPlan(pc))
//      the[NodeNotFoundException] thrownBy ExecutionPlanBuilder.buildExecutionPlan(pc)
//    }
  }
}
