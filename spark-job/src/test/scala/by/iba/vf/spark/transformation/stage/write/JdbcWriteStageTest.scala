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
package by.iba.vf.spark.transformation.stage.write

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.stage.OperationType
import by.iba.vf.spark.transformation.stage.Stage
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers._

class JdbcWriteStageTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  it("write") {
    implicit val spark: SparkSession = mock[SparkSession]

    val id = "id"
    val jdbcUrl = "jdbc:db2://testing"
    val schemaTable = "SCHEMA.TABLE"
    val user = "user"
    val password = "password"
    val query = "SCHEMA.TABLE"
    val driver = "com.ibm.db2.jcc.DB2Driver"
    val config = Map(
      JDBCOptions.JDBC_URL -> jdbcUrl,
      "user" -> user,
      "password" -> password,
      JDBCOptions.JDBC_DRIVER_CLASS -> driver,
      JDBCOptions.JDBC_TABLE_NAME -> query
    )
    val dfWriter = mock[DataFrameWriter[Row]]
    val context = mock[SparkContext]
    val df = mock[DataFrame]
    when(spark.sparkContext).thenReturn(context)
    when(df.write).thenReturn(dfWriter)
    when(df.schema).thenReturn(StructType(Array[StructField](
      StructField("n0", IntegerType),
      StructField("n1", StringType),
      StructField("n2", IntegerType),
      StructField("n3", StringType),
      StructField("n4", IntegerType)
    )))
    when(dfWriter.format(JdbcWriteStageBuilder.jdbcStorage)).thenReturn(dfWriter)
    when(dfWriter.mode("overwrite")).thenReturn(dfWriter)
    when(dfWriter.options(config + ("createTableColumnTypes" -> "n1 VARCHAR(1024),n3 VARCHAR(1024)"))).thenReturn(dfWriter)
    doNothing.when(dfWriter).save()

    doNothing.when(context).addFile("jdbc-source-truststore.jks")

    val stage = new JdbcWriteStage(Node(id, Map()), schemaTable, Some("jdbc-source-truststore.jks"), Some("overwrite"), config, TruncateMode.Simple)
    stage.write(df)
  }
}

class JdbcWriteStageBuilderTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  it("validate") {
    val config: Map[String, String] = Map(
      "operation" -> OperationType.WRITE.toString,
      "storage" -> "db2",
      "jdbcUrl" -> "jdbc:db2://testing",
      "user" -> "test",
      "password" -> "test",
      "schema" -> "SCHEMA",
      "table" -> "TABLE"
    )
    val result = JdbcWriteStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(config)
    result should be(true)
  }

  it("convert") {
    val config: Node = Node(
      "id",
      Map(
        "storage" -> "db2",
        "jdbcUrl" -> "jdbc:db2://testing",
        "user" -> "test",
        "password" -> "test",
        "schema" -> "SCHEMA",
        "table" -> "TABLE",
        "truncateMode" -> "Simple",
        "writeMode" -> "Overwrite"
      )
    )

    val result = JdbcWriteStageBuilder invokePrivate PrivateMethod[Stage]('convert)(config)

    result.getClass should be(classOf[JdbcWriteStage])
  }
}
