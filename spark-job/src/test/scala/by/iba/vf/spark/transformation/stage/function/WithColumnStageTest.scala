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
import org.apache.spark.sql.functions.{expr, lit, col, explode}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers._

class WithColumnStageTest extends AnyFunSpec with MockitoSugar with PrivateMethodTester {
  it("process") {
    implicit lazy val spark: SparkSession = mock[SparkSession]
    val df = mock[DataFrame]
    val df2 = mock[DataFrame]

    val funcVector = Vector(
      "a * 2",
      "constant",
      "string",
      "a3",
      "CASE WHEN a % 2 == 0 THEN a * 2 END",
      "count(*) OVER (PARTITION BY department)",
      "regexp_replace(a2, 1, 5)",
      "CASE WHEN a2 == 3 THEN regexp_replace(a2, a2, b) ELSE a2 END",
      "translate(a2, 'abc', 'cdb')",
      ""
    )

    val operationVector = Vector(
      "deriveColumn",
      "addConstant",
      "changeType",
      "renameColumn",
      "useConditions",
      "useWindowFunction",
      "replaceValues",
      "replaceValuesUsingConditions",
      "replaceValuesCharByChar",
      "explodeColumn"
    )
    val mapVector = Vector(
      Map("expression" -> "a * 2"),
      Map("constant" -> "constant"),
      Map("columnType" -> "string"),
      Map("columnName" -> "a3"),
      Map("conditions" -> "a % 2 == 0: a * 2"),
      Map("windowFunction" -> "count", "partitionBy" -> "department", "column" -> "*"),
      Map("oldValue" -> "1", "newValue" -> "5"),
      Map("conditions" -> "a2 == 3: a2; b", "otherwise" -> "a2"),
      Map("oldChars" -> "'abc'", "newChars" -> "'cdb'"),
      Map("" -> "")
    )

    for (i <- operationVector.indices) {
      if (Seq("deriveColumn", "useConditions", "useWindowFunction", "replaceValues",
        "replaceValuesUsingConditions", "replaceValuesCharByChar").contains(operationVector(i)))
        when(df.withColumn("a2", expr(funcVector(i)))).thenReturn(df2)
      else if (operationVector(i) == "addConstant")
        when(df.withColumn("a2", lit(funcVector(i)))).thenReturn(df2)
      else if (operationVector(i) == "changeType")
        when(df.withColumn("a2", col("a2").cast(funcVector(i)))).thenReturn(df2)
      else if (operationVector(i) == "renameColumn")
        when(df.withColumnRenamed("a2", funcVector(i))).thenReturn(df2)
      else if (operationVector(i) == "explodeColumn")
        when(df.withColumn("a2", explode(col("a2")))).thenReturn(df2)
      val stage = new WithColumnStage("id", "a2", operationVector(i), mapVector(i))
      val result = stage invokePrivate PrivateMethod[Option[DataFrame]]('process)(Map("1" -> df), spark)
      result should be(Some(df2))
    }
  }

  it("withColumn") {
    val df = mock[DataFrame]
    val df2 = mock[DataFrame]

    val funcVector = Vector(
      "a * 2",
      "constant",
      "string",
      "a3",
      "CASE WHEN a % 2 == 0 THEN a * 2 END",
      "count(*) OVER (PARTITION BY department)",
      "regexp_replace(a2, 1, 5)",
      "CASE WHEN a2 == 3 THEN regexp_replace(a2, a2, b) ELSE a2 END",
      "translate(a2, 'abc', 'cdb')",
      ""
    )

    val operationVector = Vector(
      "deriveColumn", "addConstant", "changeType", "renameColumn", "useConditions",
      "useWindowFunction", "replaceValues", "replaceValuesUsingConditions", "replaceValuesCharByChar", "explodeColumn"
    )
    val mapVector = Vector(
      Map("expression" -> "a * 2"),
      Map("constant" -> "constant"),
      Map("columnType" -> "string"),
      Map("columnName" -> "a3"),
      Map("conditions" -> "a % 2 == 0: a * 2"),
      Map("windowFunction" -> "count", "partitionBy" -> "department", "column" -> "*"),
      Map("oldValue" -> "1", "newValue" -> "5"),
      Map("conditions" -> "a2 == 3: a2; b", "otherwise" -> "a2"),
      Map("oldChars" -> "'abc'", "newChars" -> "'cdb'"),
      Map("" -> "")
    )

    for (i <- operationVector.indices) {
      if (Seq("deriveColumn", "useConditions", "useWindowFunction", "replaceValues",
        "replaceValuesUsingConditions", "replaceValuesCharByChar").contains(operationVector(i)))
        when(df.withColumn("a2", expr(funcVector(i)))).thenReturn(df2)
      else if (operationVector(i) == "addConstant")
        when(df.withColumn("a2", lit(funcVector(i)))).thenReturn(df2)
      else if (operationVector(i) == "changeType")
        when(df.withColumn("a2", col("a2").cast(funcVector(i)))).thenReturn(df2)
      else if (operationVector(i) == "renameColumn")
        when(df.withColumnRenamed("a2", funcVector(i))).thenReturn(df2)
      else if (operationVector(i) == "explodeColumn")
        when(df.withColumn("a2", explode(col("a2")))).thenReturn(df2)
      val stage = new WithColumnStage("id", "a2", operationVector(i), mapVector(i))
      val stageVector = Vector(
        stage.deriveColumn(df, "a * 2"),
        stage.addConstant(df, "constant"),
        stage.changeType(df, "string"),
        stage.renameColumn(df, "a3"),
        stage.useConditions(df, Map("conditions" -> "a % 2 == 0: a * 2")),
        stage.useWindowFunction(df, Map("windowFunction" -> "count", "partitionBy" -> "department", "column" -> "*")),
        stage.replaceValues(df, Map("oldValue" -> "1", "newValue" -> "5")),
        stage.replaceValuesUsingConditions(df, Map("conditions" -> "a2 == 3: a2; b", "otherwise" -> "a2")),
        stage.replaceValuesCharByChar(df, Map("oldChars" -> "'abc'", "newChars" -> "'cdb'")),
        stage.explodeColumn(df)
      )
      val result = stageVector(i)
      result should be(df2)
    }
  }
}

class WithColumnStageBuilderTest extends AnyFunSpec with PrivateMethodTester {
  it("validate") {
    val configVector = Vector(
      Map("operation" -> OperationType.WITH_COLUMN.toString, "column" -> "a2",
        "operationType" -> "deriveColumn", "option.expression" -> "a * 2"),
      Map("operation" -> OperationType.WITH_COLUMN.toString, "column" -> "a2",
        "operationType" -> "addConstant", "option.constant" -> "constant"),
      Map("operation" -> OperationType.WITH_COLUMN.toString, "column" -> "a2",
        "operationType" -> "changeType", "option.columnType" -> "string"),
      Map("operation" -> OperationType.WITH_COLUMN.toString, "column" -> "a2",
        "operationType" -> "renameColumn", "option.columnName" -> "a3"),
      Map("operation" -> OperationType.WITH_COLUMN.toString, "column" -> "a2",
        "operationType" -> "useConditions", "option.conditions" -> "a % 2 == 0: a * 2"),
      Map("operation" -> OperationType.WITH_COLUMN.toString, "column" -> "a2",
        "operationType" -> "useWindowFunction", "option.windowFunction" -> "count",
        "option.partitionBy" -> "department", "option.column" -> "*"),
      Map("operation" -> OperationType.WITH_COLUMN.toString, "operationType" -> "replaceValues",
        "column" -> "a2", "option.oldValue" -> "1", "option.newValue" -> "5"),
      Map("operation" -> OperationType.WITH_COLUMN.toString, "operationType" -> "replaceValuesUsingConditions",
        "column" -> "a2", "option.conditions" -> "a2 == 3: a2; b", "option.otherwise" -> "a2"),
      Map("operation" -> OperationType.WITH_COLUMN.toString, "operationType" -> "replaceValuesCharByChar",
        "column" -> "a2", "oldChars" -> "'abc'", "newChars" -> "'cdb'"),
      Map("operation" -> OperationType.WITH_COLUMN.toString, "operationType" -> "explodeColumn",
        "column" -> "a2"),
    )

    for (i <- configVector.indices) {
      val result = WithColumnStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(configVector(i))
      result should be(true)
    }
  }

  it("convert") {
    val configVector = Vector(
      Map("operation" -> OperationType.WITH_COLUMN.toString, "column" -> "a2",
        "operationType" -> "deriveColumn", "option.expression" -> "a * 2"),
      Map("operation" -> OperationType.WITH_COLUMN.toString, "column" -> "a2",
        "operationType" -> "addConstant", "option.constant" -> "constant"),
      Map("operation" -> OperationType.WITH_COLUMN.toString, "column" -> "a2",
        "operationType" -> "changeType", "option.columnType" -> "string"),
      Map("operation" -> OperationType.WITH_COLUMN.toString, "column" -> "a2",
        "operationType" -> "renameColumn", "option.columnName" -> "a3"),
      Map("operation" -> OperationType.WITH_COLUMN.toString, "column" -> "a2",
        "operationType" -> "useConditions", "option.conditions" -> "a % 2 == 0: a * 2"),
      Map("operation" -> OperationType.WITH_COLUMN.toString, "column" -> "a2",
        "operationType" -> "useWindowFunction", "option.windowFunction" -> "count",
        "option.partitionBy" -> "department", "option.column" -> "*"),
      Map("operation" -> OperationType.WITH_COLUMN.toString, "operationType" -> "replaceValues",
        "column" -> "a2", "option.oldValue" -> "1", "option.newValue" -> "5"),
      Map("operation" -> OperationType.WITH_COLUMN.toString, "operationType" -> "replaceValuesUsingConditions",
        "column" -> "a2", "option.conditions" -> "a2 == 3: a2; b", "option.otherwise" -> "a2"),
      Map("operation" -> OperationType.WITH_COLUMN.toString, "operationType" -> "replaceValuesCharByChar",
        "column" -> "a2", "oldChars" -> "'abc'", "newChars" -> "'cdb'"),
      Map("operation" -> OperationType.WITH_COLUMN.toString, "operationType" -> "explodeColumn",
        "column" -> "a2"),
    )

    for (i <- configVector.indices) {
      val config = Node("id", configVector(i))
      val result = WithColumnStageBuilder invokePrivate PrivateMethod[Stage]('convert)(config)
      result.getClass should be(classOf[WithColumnStage])
    }
  }
}
