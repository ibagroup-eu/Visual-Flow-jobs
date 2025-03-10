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
package by.iba.vf.spark.transformation.stage

import by.iba.vf.spark.transformation.config.Node
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types.{DataType, DoubleType}

trait JdbcDatabricksStageBuilder extends JdbcStageBuilder {

  JdbcDialects.registerDialect(new JdbcDialect() {
    override def canHandle(url: String): Boolean = url.toLowerCase.startsWith("jdbc:databricks:")
    override def quoteIdentifier(column: String): String = column
    override def getJDBCType(dt: DataType): Option[JdbcType] = dt match
    {
      case DoubleType => Option(JdbcType("DOUBLE", java.sql.Types.DOUBLE))
      case _ => None
    }
  })

  override val drivers = Map(
    "databricks-jdbc" -> "com.databricks.client.jdbc.Driver"
  )

  val fieldCatalog = "catalog"

  override def getFullyQualifiedTable(config: Node): String =
    s"${config.value.get(fieldCatalog).map(_ + ".").getOrElse("")}${super.getFullyQualifiedTable(config)}"

  override def buildUrl(jdbcUrl: String, password: String): String =
    s"$jdbcUrl;SSL=1;SSLKeyStore=$truststorePath;SSLKeyStorePwd=$password;UserAgentEntry=ibagroup_visual-flow/0.1;"

}
