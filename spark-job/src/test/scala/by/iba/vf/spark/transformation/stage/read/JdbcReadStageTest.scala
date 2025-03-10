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
package by.iba.vf.spark.transformation.stage.read

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.exception.TransformationConfigurationException
import by.iba.vf.spark.transformation.stage.OperationType
import by.iba.vf.spark.transformation.stage.Stage
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers._

class JdbcReadStageTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  it("read") {
    implicit val spark: SparkSession = mock[SparkSession]

    val id = "id"
    val jdbcUrl = "jdbc:db2://testing"
    val schemaTable = "SCHEMA.TABLE"
    val user = "user"
    val password = "password"
    val query = schemaTable
    val driver = "com.ibm.db2.jcc.DB2Driver"
    val numPartitions = "5"
    val config = Map(
      JDBCOptions.JDBC_URL -> jdbcUrl,
      "user" -> user,
      "password" -> password,
      JDBCOptions.JDBC_DRIVER_CLASS -> driver,
      JDBCOptions.JDBC_TABLE_NAME -> query,
      JDBCOptions.JDBC_NUM_PARTITIONS -> numPartitions
    )
    val dfReader = mock[DataFrameReader]
    val context = mock[SparkContext]
    val df = mock[DataFrame]
    when(spark.sparkContext).thenReturn(context)
    when(spark.read).thenReturn(dfReader)
    when(dfReader.format(JdbcReadStageBuilder.jdbcStorage)).thenReturn(dfReader)
    when(dfReader.options(config)).thenReturn(dfReader)
    when(dfReader.load()).thenReturn(df)

    doNothing.when(context).addFile("jdbc-source-truststore.jks")

    val optionsMap: Map[String, String] = Map.empty
    val stage = new JdbcReadStage(Node(id, Map()), schemaTable, Some("jdbc-source-truststore.jks"), config, optionsMap, false)
    val result = stage.read

    result should be(df)
  }
}

class JdbcReadStageBuilderTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  describe("buildJdbcUrl") {
  it("validate") {
    val config: Map[String, String] = Map(
      "operation" -> OperationType.READ.toString,
      "storage" -> "db2",
      "jdbcUrl" -> "jdbc:db2://testing",
      "user" -> "test",
      "password" -> "test",
      "schema" -> "SCHEMA",
      "table" -> "TABLE",
      "customSql" -> "false",
      "numPartitions" -> "5"
    )
    val result = JdbcReadStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(config)
    result should be(true)
  }
  }

  describe("convert") {
    it("driver exists") {
      val config: Node = Node(
        "id",
        Map(
          "storage" -> "db2",
          "jdbcUrl" -> "jdbc:db2://testing",
          "user" -> "test",
          "password" -> "test",
          "schema" -> "SCHEMA",
          "table" -> "TABLE",
          "customSql" -> "false",
          "numPartitions" -> "5"
        )
      )

      val result = JdbcReadStageBuilder invokePrivate PrivateMethod[Stage]('convert)(config)

      result.getClass should be(classOf[JdbcReadStage])
    }

    it("driver does not exist") {
      val config: Node = Node(
        "id",
        Map(
          "storage" -> "test",
          "jdbcUrl" -> "jdbc:test://testing",
          "user" -> "test",
          "password" -> "test",
          "certData" -> "",
          "schema" -> "SCHEMA",
          "table" -> "TABLE",
          "customSql" -> "false",
          "numPartitions" -> "5"
        )
      )

      the[TransformationConfigurationException] thrownBy (JdbcReadStageBuilder invokePrivate PrivateMethod[Stage](
        'convert
      )(config))
    }
  }
}
