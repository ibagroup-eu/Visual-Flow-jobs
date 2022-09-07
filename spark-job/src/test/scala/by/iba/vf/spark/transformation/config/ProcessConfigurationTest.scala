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
package by.iba.vf.spark.transformation.config

import org.mockito.scalatest.MockitoSugar
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers._

class ProcessConfigurationTest extends AnyFunSpec with MockitoSugar {

  private val configStr = """{
                            |  "nodes": [
                            |    {
                            |      "id": "gen-table1",
                            |      "value": {
                            |        "operation": "READ",
                            |        "storage": "GENERATOR",
                            |        "alias": "left"
                            |      }
                            |    },
                            |    {
                            |      "id": "gen-table2",
                            |      "value": {
                            |        "operation": "READ",
                            |        "storage": "GENERATOR",
                            |        "alias": "right"
                            |      }
                            |    },
                            |    {
                            |      "id": "join-data",
                            |      "value": {
                            |        "operation": "JOIN",
                            |        "joinType": "inner",
                            |        "columns": "a"
                            |      }
                            |    },
                            |    {
                            |      "id": "join-data-2",
                            |      "value": {
                            |        "operation": "JOIN",
                            |        "joinType": "inner",
                            |        "columns": "a"
                            |      }
                            |    },
                            |    {
                            |      "id": "group-by",
                            |      "value": {
                            |        "operation": "GROUP",
                            |        "groupingCriteria": "salary:avg,test:max,*:count",
                            |        "groupingColumns": "a"
                            |      }
                            |    },
                            |    {
                            |      "id": "filtering",
                            |      "value": {
                            |        "operation": "FILTER",
                            |        "condition": "left.a > 1"
                            |      }
                            |    },
                            |    {
                            |      "id": "print-2",
                            |      "value": {
                            |        "operation": "WRITE",
                            |        "storage": "STDOUT"
                            |      }
                            |    },
                            |    {
                            |      "id": "print-3",
                            |      "value": {
                            |        "operation": "WRITE",
                            |        "storage": "STDOUT"
                            |      }
                            |    },
                            |    {
                            |      "id": "print",
                            |      "value": {
                            |        "operation": "WRITE",
                            |        "storage": "STDOUT"
                            |      }
                            |    }
                            |  ],
                            |  "edges": [
                            |    {
                            |      "source": "gen-table1",
                            |      "target": "join-data"
                            |    },
                            |    {
                            |      "source": "gen-table2",
                            |      "target": "join-data"
                            |    },
                            |    {
                            |      "source": "join-data",
                            |      "target": "group-by"
                            |    },
                            |    {
                            |      "source": "join-data",
                            |      "target": "print-2"
                            |    },
                            |    {
                            |      "source": "join-data",
                            |      "target": "join-data-2"
                            |    },
                            |    {
                            |      "source": "gen-table1",
                            |      "target": "join-data-2"
                            |    },
                            |    {
                            |      "source": "join-data-2",
                            |      "target": "print-3"
                            |    },
                            |    {
                            |      "source": "group-by",
                            |      "target": "filtering"
                            |    },
                            |    {
                            |      "source": "filtering",
                            |      "target": "print"
                            |    }
                            |  ]
                            |}""".stripMargin

  private val config = ProcessConfiguration(
    List(
      Node("gen-table1", Map("operation" -> "READ", "storage" -> "GENERATOR", "alias" -> "left")),
      Node("gen-table2", Map("operation" -> "READ", "storage" -> "GENERATOR", "alias" -> "right")),
      Node("join-data", Map("operation" -> "JOIN", "joinType" -> "inner", "columns" -> "a")),
      Node("join-data-2", Map("operation" -> "JOIN", "joinType" -> "inner", "columns" -> "a")),
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
}
