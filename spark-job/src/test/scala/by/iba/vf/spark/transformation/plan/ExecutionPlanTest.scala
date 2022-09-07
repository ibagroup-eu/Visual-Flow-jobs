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
import org.mockito.scalatest.MockitoSugar
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.must.Matchers.noException

class ExecutionPlanTest extends AnyFunSpec with MockitoSugar {

  private val config = ProcessConfiguration(
    List(
      Node("gen-table1", Map("operation" -> "READ", "storage" -> "GENERATOR", "alias" -> "left")),
      Node("gen-table2", Map("operation" -> "READ", "storage" -> "GENERATOR", "alias" -> "right")),
      Node(
        "join-data",
        Map("operation" -> "JOIN", "joinType" -> "inner", "columns" -> "a", "leftDataset" -> "1", "rightDataset" -> "2")
      ),
      Node(
        "join-data-2",
        Map("operation" -> "JOIN", "joinType" -> "inner", "columns" -> "a", "leftDataset" -> "1", "rightDataset" -> "2")
      ),
      Node(
        "group-by",
        Map("operation" -> "GROUP", "groupingCriteria" -> "salary:avg,test:max,*:count", "groupingColumns" -> "a")
      ),
      Node("filtering", Map("operation" -> "FILTER", "condition" -> "left.a > 1")),
      Node("print-2", Map("operation" -> "WRITE", "storage" -> "STDOUT")),
      Node("print-3", Map("operation" -> "WRITE", "storage" -> "STDOUT")),
      Node("print", Map("operation" -> "WRITE", "storage" -> "STDOUT"))
    ),
    List(
      Edge("gen-table1", "join-data"),
      Edge("gen-table2", "join-data"),
      Edge("join-data", "group-by"),
      Edge("join-data", "print-2"),
      Edge("join-data", "join-data-2"),
      Edge("gen-table1", "join-data-2"),
      Edge("join-data-2", "print-3"),
      Edge("group-by", "filtering"),
      Edge("filtering", "print")
    )
  )

  it("printExecutionPlan") {
    val ep = ExecutionPlanBuilder.buildExecutionPlan(config)

    noException should be thrownBy ep.printExecutionPlan()
  }

}
